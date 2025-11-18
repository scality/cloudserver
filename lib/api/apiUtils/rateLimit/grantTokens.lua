-- grantTokens.lua
-- Atomically evaluates GCRA and grants tokens for rate limiting
--
-- This script implements token reservation: workers request capacity
-- in advance, and this script enforces the node-level quota using GCRA.
--
-- KEYS[1]: Counter key (e.g., "throttling:bucket:mybucket:rps")
-- ARGV[1]: Requested token count (number of requests)
-- ARGV[2]: Interval per request in milliseconds
-- ARGV[3]: Burst capacity in milliseconds (bucket size)
-- ARGV[4]: Current timestamp in milliseconds (arrivedAt)
--
-- Returns: Number of tokens granted (0 if quota exhausted, partial if limited)

local key = KEYS[1]
local requested = tonumber(ARGV[1])
local interval = tonumber(ARGV[2])
local burstCapacity = tonumber(ARGV[3])
local arrivedAt = tonumber(ARGV[4])

-- Get current counter value (emptyAt timestamp)
local emptyAt = tonumber(redis.call('GET', key) or 0)

-- GCRA evaluation
-- expectedTime: When the bucket will be empty (or now if already empty)
local expectedTime = math.max(emptyAt, arrivedAt)

-- Calculate cost for requested tokens
local cost = requested * interval

-- Check if request fits within burst capacity
-- Allow if: expectedTime + cost <= arrivedAt + burstCapacity
local allowAt = expectedTime + cost
local burstLimit = arrivedAt + burstCapacity

if allowAt <= burstLimit then
    -- Full request allowed
    local newEmptyAt = expectedTime + cost
    redis.call('SET', key, newEmptyAt)
    redis.call('PEXPIRE', key, burstCapacity + 10000) -- TTL = burst + 10s buffer

    return requested
else
    -- Request exceeds capacity, grant partial tokens
    -- available = (arrivedAt + burstCapacity - expectedTime) / interval
    local availableCapacity = burstLimit - expectedTime

    if availableCapacity > 0 then
        -- Grant partial tokens
        local granted = math.floor(availableCapacity / interval)

        if granted > 0 then
            local actualCost = granted * interval
            local newEmptyAt = expectedTime + actualCost
            redis.call('SET', key, newEmptyAt)
            redis.call('PEXPIRE', key, burstCapacity + 10000)

            return granted
        end
    end

    -- No capacity available
    return 0
end
