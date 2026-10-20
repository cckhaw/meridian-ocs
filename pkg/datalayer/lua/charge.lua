-- Atomic credit-control step for one subscriber: settle usage, refund the
-- unused part of the previous reservation, reserve the next slice, record the
-- session state and the answer, all in one script.
--
-- KEYS[1] = subscriber prefix
-- ARGV: mode (I|U|T|E|L|N), sid, reqNo, nowMs, validityS, graceMs, nItems,
--       then per item: rg, kind, requested, used, nRates, (cls, num, den) * nRates
--
-- Reply (flat integers): code, nItems, then per item
--       rg, result, granted, validityS, final, shortfall
--   code:   0 ok | 1 ok (replayed stored answer) | 2 unknown or terminated session | 3 stale request
--
-- Modes L and N settle the usage of an UPDATE that was refused while the data tier was down, after the fact. They
-- settle the open reservation of the rating group against the reported usage (charging what exceeds it, as an
-- update does) but reserve nothing; the answer stored for the request is a refusal. Both are idempotent (a marker
-- key, P:late:<sid>:<reqNo>), both advance the session to reqNo when it is ahead (so a retransmission of the refused
-- request is answered with the refusal and charges nothing twice), and neither ever charges a request the data
-- tier already applied.
--   N  (exact, the default) The session remembers which of its last 8 request numbers were applied: every request
--      processed normally leaves a field a:<reqNo>, the field of reqNo-8 is removed, and field w says the session
--      keeps this record. N finds a:<reqNo> (or reqNo == last): the request WAS applied, by the original call or by
--      a retransmission after the outage, so nothing is charged. It finds neither and reqNo is within 8 of last (or
--      ahead of it): the request was NOT applied, whether or not the session moved on since, so its usage is
--      charged. Older than that window, or a session that predates the record: whether it was applied can no longer
--      be told, nothing is charged (code 3) and a person decides.
--   L  (strict, for peers that may report refused usage again under a new request number) Applied only if no
--      request with that number or a later one has been processed (reqNo > last); at that number the original was
--      applied (nothing to charge); past it the usage may have been reported again, so nothing is charged (code 3).
--   result: 0 granted | 1 granted-partial + final | 2 limit reached | 3 no eligible bucket
--           | 4 subscriber barred (suspended or terminated)

local P = KEYS[1]
local mode, sid = ARGV[1], ARGV[2]
local reqNo, now = tonumber(ARGV[3]), tonumber(ARGV[4])
local validity, grace = tonumber(ARGV[5]), tonumber(ARGV[6])
local nItems = tonumber(ARGV[7])

sweep(P, now)

local sk = P .. ':s:' .. sid

local function replay()
  local out = {}
  for v in string.gmatch(redis.call('HGET', sk, 'ans'), '[^,]+') do
    out[#out + 1] = tonumber(v)
  end
  out[1] = 1 -- mark as replayed
  return out
end

-- Idempotency: (session, request number) mutates state at most once.
local last = redis.call('HGET', sk, 'last')
local late = (mode == 'L' or mode == 'N')
local marker = P .. ':late:' .. sid .. ':' .. fmt(reqNo)
if late and redis.call('EXISTS', marker) == 1 then return { 1, 0 } end
if mode == 'I' or mode == 'E' then
  if last then
    if tonumber(last) == reqNo then return replay() end
    return { 3 }
  end
elseif mode == 'N' then
  if not last then return { 2 } end
  local l = tonumber(last)
  if reqNo == l or redis.call('HEXISTS', sk, 'a:' .. fmt(reqNo)) == 1 then return { 1, 0 } end -- applied already
  if reqNo < l and (reqNo < l - 7 or redis.call('HEXISTS', sk, 'w') == 0) then return { 3 } end -- cannot be told
else
  if not last then return { 2 } end
  local l = tonumber(last)
  if reqNo == l then return replay() end
  if reqNo < l then return { 3 } end
  -- A terminated session is gone: nothing may reserve quota on it again.
  if redis.call('HEXISTS', sk, 'closed') == 1 then return { 2 } end
end

-- A suspended or terminated subscriber gets no new quota or events charged.
-- Usage already consumed is still settled. A subscriber with no profile
-- (provisioned before profiles existed) is served.
local barred = false
if mode ~= 'T' then
  local st = redis.call('HGET', P .. ':profile', 'status')
  barred = (st == 'suspended' or st == 'terminated')
end

local expMs = now + validity * 1000 + grace
local out = { 0, nItems }
local pos = 8

-- A terminate without usage items (e.g. CCR-T with no MSCC) must still release
-- everything the session holds: refund every open reservation in full.
if mode == 'T' and nItems == 0 then
  for _, field in ipairs(redis.call('HKEYS', sk)) do
    if string.sub(field, 1, 2) == 'r:' then
      local resId = redis.call('HGET', sk, field)
      local rk = P .. ':r:' .. resId
      local f = redis.call('HMGET', rk, 'units', 'dbt')
      if f[1] then refund_tail(P, parse_debits(f[2]), tonumber(f[1])) end
      redis.call('DEL', rk)
      redis.call('ZREM', P .. ':rx', resId)
      redis.call('HDEL', sk, field)
    end
  end
end

for _ = 1, nItems do
  local rg, kind = ARGV[pos], ARGV[pos + 1]
  local requested, used = tonumber(ARGV[pos + 2]), tonumber(ARGV[pos + 3])
  local nr = tonumber(ARGV[pos + 4])
  pos = pos + 5
  local rates = {}
  for _ = 1, nr do
    rates[ARGV[pos]] = { tonumber(ARGV[pos + 1]), tonumber(ARGV[pos + 2]) }
    pos = pos + 3
  end

  local shortfall, committed = 0, nil
  local reserved = 0

  -- 1. Settle the previous reservation for this rating group.
  if mode == 'U' or mode == 'T' or late then
    local debits = {}
    local resId = redis.call('HGET', sk, 'r:' .. rg)
    if resId then
      local rk = P .. ':r:' .. resId
      local f = redis.call('HMGET', rk, 'units', 'dbt')
      if f[1] then
        reserved, debits = tonumber(f[1]), parse_debits(f[2])
      end
      redis.call('DEL', rk)
      redis.call('ZREM', P .. ':rx', resId)
      redis.call('HDEL', sk, 'r:' .. rg)
    end
    if used < reserved then
      refund_tail(P, debits, reserved - used)
    elseif used > reserved then
      -- Over-use (or the reservation was already expired): charge what we can,
      -- report the rest as shortfall for the caller to adjust.
      local extra = used - reserved
      local got, extraDebits = allocate(P, kind, extra, rates, now)
      shortfall = extra - got
      for _, d in ipairs(extraDebits) do debits[#debits + 1] = d end
    end
    committed = debits
  end

  -- 2. Reserve the next slice (or charge an event outright).
  local granted, result, final = 0, 0, 0
  if barred and mode ~= 'T' then
    result = 4
  elseif mode == 'I' or mode == 'U' then
    local got, nd, eligible = allocate(P, kind, requested, rates, now)
    granted = got
    if got <= 0 then
      result = eligible and 2 or 3
    else
      if got < requested then result, final = 1, 1 end
      local resId = sid .. ':' .. rg .. ':' .. fmt(reqNo)
      redis.call('HSET', P .. ':r:' .. resId, 'sid', sid, 'rg', rg,
                 'units', fmt(got), 'dbt', enc_debits(nd))
      redis.call('ZADD', P .. ':rx', expMs, resId)
      redis.call('HSET', sk, 'r:' .. rg, resId)
    end
  elseif late then
    result = 2 -- nothing is reserved: a retransmission of this request is answered with the refusal it first got
  elseif mode == 'E' then
    local got, nd, eligible = allocate(P, kind, requested, rates, now)
    if got < requested then
      refund_tail(P, nd, got) -- all or nothing
      result = eligible and 2 or 3
    else
      granted, committed = got, nd
    end
  end

  if committed then
    ledger(P, 'type', (mode == 'E') and 'event' or (late and 'late' or 'settle'), 'sid', sid, 'rg', rg,
           'used', fmt(mode == 'E' and granted or used), 'reserved', fmt(reserved),
           'shortfall', fmt(shortfall), 'dbt', enc_debits(committed), 'at', fmt(now))
  end

  local vs = (granted > 0 and mode ~= 'E') and validity or 0
  out[#out + 1] = tonumber(rg)
  out[#out + 1] = result
  out[#out + 1] = granted
  out[#out + 1] = vs
  out[#out + 1] = final
  out[#out + 1] = shortfall
end

-- 3. Persist session state and the stored answer.
local packed = {}
for i = 1, #out do packed[i] = fmt(out[i]) end
-- Remember which request numbers were applied (the last 8): see the header.
local fields = { 'a:' .. fmt(reqNo), '1' }
if mode == 'I' then fields[#fields + 1] = 'w'; fields[#fields + 1] = '1' end
if not late or reqNo > tonumber(last) then
  fields[#fields + 1] = 'last'; fields[#fields + 1] = fmt(reqNo)
  fields[#fields + 1] = 'ans'; fields[#fields + 1] = table.concat(packed, ',')
end
redis.call('HSET', sk, unpack(fields))
if reqNo >= 8 then redis.call('HDEL', sk, 'a:' .. fmt(reqNo - 8)) end
if mode == 'T' then redis.call('HSET', sk, 'closed', '1') end
local ttl = (mode == 'T' or mode == 'E') and 3600000 or (validity * 1000 + grace + 3600000)
redis.call('PEXPIRE', sk, ttl)
if late then redis.call('SET', marker, '1', 'PX', ttl) end

return out
