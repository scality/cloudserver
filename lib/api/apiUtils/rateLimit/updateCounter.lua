-- updateCounter <KEY> <COST>
--
-- Adds the passed COST to the GCRA counter at KEY.
-- If no counter currently exists a new one is created from the current time.
-- The key expiration is set to the updated value.
-- Returns the value of the updated key.

local ts = redis.call('TIME')
local currentTime = ts[1] * 1000
currentTime = currentTime + math.floor(ts[2] / 1000)

local newValue = currentTime + tonumber(ARGV[1])

local counterExists = redis.call('EXISTS', KEYS[1])
if counterExists == 1 then
    local currentValue = tonumber(redis.call('GET', KEYS[1]))
    if currentValue > currentTime then
        newValue = currentValue + tonumber(ARGV[1])
    end
end

redis.call('SET', KEYS[1], newValue)

local expiry = math.ceil(newValue / 1000)
redis.call('EXPIREAT', KEYS[1], expiry)

return newValue
