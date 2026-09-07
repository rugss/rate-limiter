--calculate window start time
-- delete all older requests before window start time
--check if the number of requests in the current window >= limit reject
--else add it in your window and move forward

--key  = rate_limiter key(rate_limiter:<user>:<user_id>)
--argv = window size,limit,current time, request id

local rate_limiter_key = KEYS[1]

local window_size = tonumber(ARGV[1])
local limit = tonumber(ARGV[2])
local request_time = tonumber(ARGV[3])
local request_id = ARGV[4]


--calculate window start time

local window_start_time = request_time-window_size 


--delete all older requests before window start time
redis.call('ZREMRANGEBYSCORE',rate_limiter_key,0,window_start_time)


--check if the number of requests in the current window >= limit reject
local request_present_in_window = redis.call('ZCARD',rate_limiter_key)


if request_present_in_window >= limit then
    return {0,0}

end


--else add it in your window and move forward
redis.call('ZADD',rate_limiter_key,request_time,request_id)
redis.call('PEXPIRE',rate_limiter_key,window_size)

return {1,limit-request_present_in_window-1} --1 = allowed, number of reqs remaning