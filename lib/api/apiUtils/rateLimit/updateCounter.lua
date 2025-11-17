-- updateCounter <KEY> <COST>
--
-- Adds the passed COST to the GCRA counter at KEY.
-- Returns the value BEFORE adding this worker's contribution (prevents accumulation race).
-- This enables fair worker coordination when syncs are staggered.

local ts = redis.call('TIME')
local currentTime = ts[1] * 1000
currentTime = currentTime + math.floor(ts[2] / 1000)

-- Determine value before this update (what we'll return)
local valueBeforeUpdate = currentTime
local counterExists = redis.call('EXISTS', KEYS[1])
if counterExists == 1 then
    local currentValue = tonumber(redis.call('GET', KEYS[1]))
    if currentValue > currentTime then
        valueBeforeUpdate = currentValue
    end
end

-- Calculate new value by adding the cost
local newValue = valueBeforeUpdate + tonumber(ARGV[1])

-- Store updated value
redis.call('SET', KEYS[1], newValue)

-- Expire counter after 60 seconds of inactivity
-- This is longer than the 10s sync interval to ensure counters persist
redis.call('EXPIRE', KEYS[1], 60)

-- Return value BEFORE this update to prevent accumulation race
-- This way early-syncing workers don't get unfair advantage
return valueBeforeUpdate
