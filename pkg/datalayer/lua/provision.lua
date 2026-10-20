-- Atomic, idempotent provisioning step for one subscriber: create, top up,
-- adjust, add / reload / remove a bucket, change lifecycle state.
--
-- KEYS[1] = subscriber prefix (as for charge.lua)
-- ARGV: op, token, fingerprint, nowMs, idemTtlMs, then per op (from ARGV[6]):
--   C  payment, status, ids   (ids: comma separated "type:value" list, may be empty; kept on the
--      profile so a subscriber can be displayed with its identifiers)
--   L  target, untilMs                      (untilMs > 0 only used for grace)
--   T  bucketId, amount, order, allowNeg    (credit a cash bucket; created if missing)
--   A  bucketId, delta, order, allowNeg     (credit or debit any bucket)
--   B  bucketId, class, kind, amount, expMs, order
--   R  bucketId, amount, expMs|-1, order|keep   (reset an allowance)
--   X  bucketId, force
--   P  offering, renew, n, then n x (bucketId, class, kind, amount, expMs, order)
--      attach an offering: set the subscriber's offering and create its allowances,
--      all or nothing. With renew=1 an existing allowance bucket is reset instead
--      of being an error.
--
-- Reply: {code, values...}, every value a string.
--   0 applied | 1 duplicate (values are the stored original: code, values...)
--   2 no such subscriber | 3 already exists | 4 invalid transition (from, to)
--   5 insufficient balance (balance) | 6 token reused with a different request
--   7 no such bucket | 8 subscriber terminated | 9 balance overflow
--   10 bucket not empty (balance) | 11 not a cash bucket
--   12 allowance bucket already exists (bucket id), nothing applied
--
-- The outcome of every request, failures included, is stored under the token,
-- so a redelivered command gets the same answer even if state has changed.

local P = KEYS[1]
local op, token, fp = ARGV[1], ARGV[2], ARGV[3]
local now, idemTtl = tonumber(ARGV[4]), tonumber(ARGV[5])
local pk = P .. ':profile'
local ik = P .. ':idem:' .. token
local MAXBAL = 4503599627370496 -- 2^52: balances stay exact in a Lua double

local prev = redis.call('HMGET', ik, 'fp', 'res')
if prev[1] then
  if prev[1] ~= fp then return { 6 } end
  local out = { 1 }
  for v in string.gmatch(prev[2], '[^,]+') do out[#out + 1] = v end
  return out
end

local function finish(code, vals)
  local out, stored = { code }, { fmt(code) }
  for _, v in ipairs(vals or {}) do
    out[#out + 1] = v
    stored[#stored + 1] = v
  end
  redis.call('HSET', ik, 'fp', fp, 'res', table.concat(stored, ','))
  redis.call('PEXPIRE', ik, idemTtl)
  return out
end

local function bump() return redis.call('HINCRBY', pk, 'ver', 1) end

if op == 'C' then
  local payment, status, ids = ARGV[6], ARGV[7], ARGV[8]
  if redis.call('EXISTS', pk) == 1 then return finish(3) end
  redis.call('HSET', pk, 'payment', payment, 'status', status,
             'createdAt', fmt(now), 'statusAt', fmt(now), 'ver', '1')
  if ids and ids ~= '' then redis.call('HSET', pk, 'ids', ids) end
  ledger(P, 'type', 'create', 'payment', payment, 'token', token, 'at', fmt(now))
  return finish(0, { '1' })
end

local status = redis.call('HGET', pk, 'status')
if not status then return finish(2) end

if op == 'L' then
  local to, untilMs = ARGV[6], tonumber(ARGV[7])
  local allowed = ({
    active = { grace = 1, suspended = 1, terminated = 1 },
    grace = { active = 1, suspended = 1, terminated = 1 },
    suspended = { active = 1, terminated = 1 },
    terminated = {},
  })[status]
  if not allowed or not allowed[to] then return finish(4, { status, to }) end
  redis.call('HSET', pk, 'status', to, 'statusAt', fmt(now))
  if to == 'grace' and untilMs > 0 then
    redis.call('HSET', pk, 'graceUntil', fmt(untilMs))
  else
    redis.call('HDEL', pk, 'graceUntil')
  end
  local ver = bump()
  ledger(P, 'type', 'lifecycle', 'from', status, 'to', to, 'token', token, 'at', fmt(now))
  return finish(0, { status, to, fmt(ver) })
end

if status == 'terminated' then return finish(8) end

if op == 'T' or op == 'A' then
  local id, delta, order, allowNeg = ARGV[6], tonumber(ARGV[7]), tonumber(ARGV[8]), ARGV[9]
  local bk = P .. ':b:' .. id
  if redis.call('EXISTS', bk) == 0 then
    if delta < 0 then return finish(7) end
    redis.call('HSET', bk, 'cls', 'cash', 'kind', 'any', 'bal', '0', 'exp', '0')
    redis.call('ZADD', P .. ':ord', order, id)
  elseif op == 'T' and redis.call('HGET', bk, 'cls') ~= 'cash' then
    return finish(11)
  end
  local bal = tonumber(redis.call('HGET', bk, 'bal'))
  local nb = bal + delta
  if nb < 0 and allowNeg ~= '1' then return finish(5, { fmt(bal) }) end
  if nb > MAXBAL then return finish(9) end
  redis.call('HINCRBY', bk, 'bal', ARGV[7])
  local ver = bump()
  ledger(P, 'type', op == 'T' and 'topup' or 'adjust', 'bucket', id, 'delta', ARGV[7],
         'balance', fmt(nb), 'token', token, 'at', fmt(now))
  return finish(0, { fmt(nb), fmt(ver) })
end

if op == 'B' then
  local id, cls, kind, amount, exp, order = ARGV[6], ARGV[7], ARGV[8], tonumber(ARGV[9]), ARGV[10], ARGV[11]
  local bk = P .. ':b:' .. id
  if redis.call('EXISTS', bk) == 1 then return finish(3) end
  if amount > MAXBAL then return finish(9) end
  redis.call('HSET', bk, 'cls', cls, 'kind', kind, 'bal', ARGV[9], 'exp', exp)
  redis.call('ZADD', P .. ':ord', tonumber(order), id)
  local ver = bump()
  ledger(P, 'type', 'bundle', 'bucket', id, 'class', cls, 'amount', ARGV[9], 'token', token, 'at', fmt(now))
  return finish(0, { fmt(amount), fmt(ver) })
end

if op == 'R' then
  local id, amount, exp, order = ARGV[6], tonumber(ARGV[7]), ARGV[8], ARGV[9]
  local bk = P .. ':b:' .. id
  if redis.call('EXISTS', bk) == 0 then return finish(7) end
  if amount > MAXBAL then return finish(9) end
  local old = tonumber(redis.call('HGET', bk, 'bal'))
  redis.call('HSET', bk, 'bal', ARGV[7])
  if exp ~= '-1' then redis.call('HSET', bk, 'exp', exp) end
  if order ~= 'keep' then redis.call('ZADD', P .. ':ord', tonumber(order), id) end
  local ver = bump()
  ledger(P, 'type', 'reload', 'bucket', id, 'from', fmt(old), 'to', ARGV[7], 'token', token, 'at', fmt(now))
  return finish(0, { fmt(amount), fmt(old), fmt(ver) })
end

if op == 'X' then
  local id, force = ARGV[6], ARGV[7]
  local bk = P .. ':b:' .. id
  if redis.call('EXISTS', bk) == 0 then return finish(7) end
  local bal = tonumber(redis.call('HGET', bk, 'bal'))
  if bal > 0 and force ~= '1' then return finish(10, { fmt(bal) }) end
  redis.call('DEL', bk)
  redis.call('ZREM', P .. ':ord', id)
  local ver = bump()
  ledger(P, 'type', 'remove', 'bucket', id, 'balance', fmt(bal), 'token', token, 'at', fmt(now))
  return finish(0, { fmt(bal), fmt(ver) })
end

if op == 'P' then
  local offering, renew, n = ARGV[6], ARGV[7], tonumber(ARGV[8])
  local base = 9
  if not n or n < 0 or n > 32 then error('provision: bad allowance count') end
  -- Check everything before writing anything.
  for i = 0, n - 1 do
    local at = base + i * 6
    local bk = P .. ':b:' .. ARGV[at]
    if redis.call('EXISTS', bk) == 1 then
      if renew ~= '1' then return finish(12, { ARGV[at] }) end
      if redis.call('HGET', bk, 'cls') ~= ARGV[at + 1] then return finish(12, { ARGV[at] }) end
    end
    if tonumber(ARGV[at + 3]) > MAXBAL then return finish(9) end
  end
  for i = 0, n - 1 do
    local at = base + i * 6
    local id, cls, kind, amount, exp, order = ARGV[at], ARGV[at + 1], ARGV[at + 2], ARGV[at + 3], ARGV[at + 4], ARGV[at + 5]
    local bk = P .. ':b:' .. id
    redis.call('HSET', bk, 'cls', cls, 'kind', kind, 'bal', amount, 'exp', exp)
    redis.call('ZADD', P .. ':ord', tonumber(order), id)
  end
  -- '-' stands for "none": empty values do not survive the stored reply (it is comma joined)
  local old = redis.call('HGET', pk, 'offering') or '-'
  redis.call('HSET', pk, 'offering', offering)
  local ver = bump()
  ledger(P, 'type', 'offering', 'from', old, 'to', offering, 'buckets', fmt(n), 'token', token, 'at', fmt(now))
  return finish(0, { old, offering, fmt(n), fmt(ver) })
end

error('provision: unknown op ' .. tostring(op))
