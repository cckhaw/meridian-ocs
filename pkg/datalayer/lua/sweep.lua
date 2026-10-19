-- KEYS[1] = subscriber prefix, ARGV[1] = now (unix ms)
-- Returns the number of expired reservations processed (max 64 per call).
return sweep(KEYS[1], tonumber(ARGV[1]))
