-- Shared helpers, concatenated in front of every script by the Go loader.
--
-- Key layout (P is KEYS[1], e.g. "{sub:4670000001}"; every key shares the hash
-- tag so one subscriber lives in one cluster slot):
--   P:ord          ZSET  bucket id -> consumption order (lowest score first)
--   P:b:<id>       HASH  cls (promo|bundle|cash), kind (bytes|seconds|events|any),
--                        bal (int), exp (unix ms, 0 = never)
--   P:s:<sid>      HASH  last (last request number), ans (packed answer),
--                        r:<rg> (open reservation id for rating group rg)
--   P:r:<resId>    HASH  sid, rg, units (reserved), dbt (debit list)
--   P:rx           ZSET  open reservation id -> expiry (unix ms)
--   P:led          STREAM capped ledger-intent log (recovery aid; the engine also
--                        publishes to Kafka)
--   P:orph         HASH  refunds whose bucket no longer exists (never silently lost)
--
-- All quantities are Lua doubles, so they must stay below 2^52 and rate
-- denominators below 2^12; the Go client enforces these bounds.

local function fmt(n) return string.format('%d', n) end

-- cost of `units` at num/den, rounded up in the operator's favour.
local function cost_of(units, num, den)
  if num == den then return units end
  return math.ceil(units * num / den)
end

-- most units a balance can pay for at num/den, capped at `want` when cheap to say so.
local function max_units(bal, num, den, want)
  if num == den then return bal end
  local q = math.floor(bal / num)
  if q * num > bal then q = q - 1 end
  if q >= want then return want end
  local r = bal - q * num
  return q * den + math.floor(r * den / num)
end

local function enc_debits(t)
  local out = {}
  for i, d in ipairs(t) do
    out[i] = d.id .. '|' .. fmt(d.cost) .. '|' .. fmt(d.units) .. '|' .. fmt(d.num) .. '|' .. fmt(d.den)
  end
  return table.concat(out, ';')
end

local function parse_debits(str)
  local t = {}
  if not str or str == '' then return t end
  for rec in string.gmatch(str, '[^;]+') do
    local id, cost, units, num, den = string.match(rec, '^([^|]+)|(%-?%d+)|(%d+)|(%d+)|(%d+)$')
    t[#t + 1] = { id = id, cost = tonumber(cost), units = tonumber(units),
                  num = tonumber(num), den = tonumber(den) }
  end
  return t
end

local function ledger(P, ...)
  redis.call('XADD', P .. ':led', 'MAXLEN', '~', '256', '*', ...)
end

-- Debit up to `want` units across eligible buckets in consumption order.
-- rates: class -> {num, den}. Returns got, debits, eligible.
local function allocate(P, kind, want, rates, now)
  local debits, remaining, eligible = {}, want, false
  if want <= 0 then return 0, debits, false end
  local ids = redis.call('ZRANGE', P .. ':ord', 0, -1)
  for _, id in ipairs(ids) do
    if remaining <= 0 then break end
    local bk = P .. ':b:' .. id
    local f = redis.call('HMGET', bk, 'cls', 'kind', 'bal', 'exp')
    local cls, bkind, bal, exp = f[1], f[2], tonumber(f[3]), tonumber(f[4]) or 0
    local r = cls and rates[cls]
    if r and bal and (bkind == kind or bkind == 'any') and (exp == 0 or exp > now) then
      eligible = true
      if bal > 0 then
        local num, den = r[1], r[2]
        local take = math.min(remaining, max_units(bal, num, den, remaining))
        if take > 0 then
          local cost = cost_of(take, num, den)
          redis.call('HINCRBY', bk, 'bal', -cost)
          debits[#debits + 1] = { id = id, cost = cost, units = take, num = num, den = den }
          remaining = remaining - take
        end
      end
    end
  end
  return want - remaining, debits, eligible
end

-- Give `unused` units back, newest debit first. Mutates the debit list so it
-- afterwards describes what was actually committed. Returns units not refunded.
local function refund_tail(P, debits, unused)
  for i = #debits, 1, -1 do
    if unused <= 0 then break end
    local d = debits[i]
    local ru = math.min(unused, d.units)
    if ru > 0 then
      local keep_units = d.units - ru
      local keep_cost = cost_of(keep_units, d.num, d.den)
      local back = d.cost - keep_cost
      if back > 0 then
        local bk = P .. ':b:' .. d.id
        if redis.call('EXISTS', bk) == 1 then
          redis.call('HINCRBY', bk, 'bal', back)
        else
          redis.call('HINCRBY', P .. ':orph', d.id, back)
        end
      end
      d.units, d.cost = keep_units, keep_cost
      unused = unused - ru
    end
  end
  return unused
end

-- Refund in full every reservation whose expiry has passed (silent session drop).
local function sweep(P, now)
  local due = redis.call('ZRANGEBYSCORE', P .. ':rx', '-inf', now, 'LIMIT', 0, 64)
  for _, resId in ipairs(due) do
    local rk = P .. ':r:' .. resId
    local f = redis.call('HMGET', rk, 'sid', 'rg', 'units', 'dbt')
    if f[1] then
      refund_tail(P, parse_debits(f[4]), tonumber(f[3]))
      local sk = P .. ':s:' .. f[1]
      if redis.call('HGET', sk, 'r:' .. f[2]) == resId then
        redis.call('HDEL', sk, 'r:' .. f[2])
      end
      redis.call('DEL', rk)
      ledger(P, 'type', 'expire', 'sid', f[1], 'rg', f[2], 'reserved', f[3], 'at', fmt(now))
    end
    redis.call('ZREM', P .. ':rx', resId)
  end
  return #due
end
