-- GCRA Counter Update with Delay Merging (from POC)
--
-- This Lua script implements atomic GCRA counter synchronization using delay merging
-- for fair quota distribution across multiple Node.js workers on the same node.
--
-- ARCHITECTURE:
--   - 10 Node.js workers per physical node share 1 local Redis instance
--   - Each worker maintains in-memory TAT (Theoretical Arrival Time)
--   - Workers sync TAT to Redis every 10 seconds
--   - Lua script MERGES delays from Redis and worker
--
-- DELAY MERGING ALGORITHM:
--   Given:
--     - now: Current timestamp
--     - redisTAT: TAT stored in Redis (from other workers)
--     - workerTAT: TAT from this worker
--
--   Calculate:
--     - localDelay = redisTAT - now (how far ahead is Redis?)
--     - remoteDelay = workerTAT - now (how far ahead is this worker?)
--     - mergedTAT = localDelay + remoteDelay + now (accumulate both delays)
--
--   Example with 2 workers:
--     - now = 1000ms
--     - Worker 1 syncs: workerTAT = 1100ms (delay = 100ms)
--       → redisTAT = 1100ms
--     - Worker 2 syncs: workerTAT = 1100ms (delay = 100ms)
--       → localDelay = 1100 - 1000 = 100ms
--       → remoteDelay = 1100 - 1000 = 100ms
--       → mergedTAT = 100 + 100 + 1000 = 1200ms ✅ Both workers counted!
--
-- WHY NOT "MAX WINS"?
--   Max wins would be: redisTAT = max(1100, 1100) = 1100ms
--   Result: Only 1 worker's quota counted, 10x over-allocation!
--
-- ============================================================================
--
-- Args:
--   KEYS[1] - Counter key (e.g., "throttling:bucket:my-bucket:rps")
--   ARGV[1] - now (current timestamp in milliseconds)
--   ARGV[2] - workerTAT (this worker's TAT in milliseconds)
--
-- Returns:
--   Merged TAT value after combining delays

local counterExists = redis.call('EXISTS', KEYS[1])

if counterExists ~= 1 then
    -- First write: initialize with worker's TAT
    redis.call('SET', KEYS[1], ARGV[2])

    -- Set expiry to 60 seconds after TAT
    local expiry = math.ceil(tonumber(ARGV[2]) / 1000) + 60
    redis.call('EXPIREAT', KEYS[1], expiry)

    return ARGV[2]
else
    -- Counter exists: merge delays from Redis and worker
    local currentValue = tonumber(redis.call('GET', KEYS[1]))
    local now = tonumber(ARGV[1])
    local workerTAT = tonumber(ARGV[2])

    -- Calculate delays
    local localDelay = currentValue - now      -- How far ahead is Redis?
    local remoteDelay = workerTAT - now        -- How far ahead is worker?

    -- Merge delays: accumulate both
    local newCounter = localDelay + remoteDelay + now

    -- Update Redis with merged value
    redis.call('SET', KEYS[1], newCounter)

    -- Set expiry
    local expiry = math.ceil(newCounter / 1000) + 60
    redis.call('EXPIREAT', KEYS[1], expiry)

    return newCounter
end
