-- This Lua script provides atomic GCRA counter synchronization in Redis for
-- sharing rate limit state between Node.js workers on the same physical node.
--
-- ARCHITECTURE:
--   - 10 Node.js workers per physical node
--   - All workers share 1 local Redis instance
--   - Workers sync TAT (Theoretical Arrival Time) to Redis
--   - Workers read from Redis on cache miss
--
-- ============================================================================
--
-- updateCounter <KEY> <TAT>
--
-- Syncs the TAT (Theoretical Arrival Time) to Redis for the given key.
-- Only updates if the provided TAT is newer than the current value in Redis.
-- This prevents out-of-order updates from overwriting newer state.
--
-- Args:
--   KEYS[1] - Counter key (e.g., "throttling:bucket:my-bucket:rps")
--   ARGV[1] - TAT timestamp in milliseconds
--
-- Returns:
--   Current TAT value in Redis after update

local newTAT = tonumber(ARGV[1])

-- Get current TAT from Redis (or 0 if not exists)
local currentTAT = tonumber(redis.call('GET', KEYS[1])) or 0

-- Only update if new TAT is newer (prevents stale updates)
if newTAT > currentTAT then
    redis.call('SET', KEYS[1], newTAT)

    -- Set expiry to 60 seconds after TAT
    -- This allows counter to persist briefly after bucket empties
    local expiry = math.ceil(newTAT / 1000) + 60
    redis.call('EXPIREAT', KEYS[1], expiry)
end

-- Return current value (may be newer than provided TAT if another worker updated)
return redis.call('GET', KEYS[1])
