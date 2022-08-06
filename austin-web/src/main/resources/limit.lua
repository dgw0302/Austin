




--KEYS[1]: 限流 key
--ARGV[1]: 限流窗口,毫秒
--ARGV[2]: 当前时间戳（作为score）
--ARGV[3]: 阈值
--ARGV[4]: score 对应的唯一value
-- 1\. 移除开始时间窗口之前的数据
--Redis zremrangebyscore，命令用于移除有序集 key 中，所有 score 值介于 min 和 max 之间(包括等于 min 或 max )的成员。
--这里的意思就是移除当前时间减去活动窗口大小的时间之前的数据
redis.call('zremrangeByScore', KEYS[1], 0, ARGV[2]-ARGV[1])
-- 2\. 统计当前元素数量
--Redis ZCARD 命令用于返回有序集的成员个数。
local res = redis.call('zcard', KEYS[1])
-- 3\. 是否超过阈值
if (res == nil) or (res < tonumber(ARGV[3])) then
    redis.call('zadd', KEYS[1], ARGV[2], ARGV[4])
    redis.call('expire', KEYS[1], ARGV[1]/1000)
    return 0
else
    return 1
end
