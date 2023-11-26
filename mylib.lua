#!lua name=mylib
redis.register_function('add_wc', 
function(KEYS, ARGV) 
    local sta = redis.call('XACK',KEYS[2],KEYS[3],KEYS[4])
    local sta2 = 0
    if sta == 1 then
        for i = 1, #ARGV, 2 do
            redis.call("ZINCRBY", KEYS[1], ARGV[i], ARGV[i+1])
        end
        redis.call('XADD', KEYS[5], '*', KEYS[6], KEYS[7])
        redis.call('HSET',KEYS[8],KEYS[7],KEYS[9])
        local start = redis.call('HGET',KEYS[10],KEYS[7])
        local end1 = redis.call('HGET',KEYS[8],KEYS[7])
        local diff = KEYS[9] - start
        redis.call('HSET',KEYS[8],KEYS[7],diff)
        -- redis.call('HSET',KEYS[10],KEYS[7],end1)
        return diff
    end
    return sta2
end
)
redis.register_function('check',
function(KEYS, ARGV)
    local sta = redis.call('HGET',KEYS[1],KEYS[2])
    return sta
end
)
redis.register_function('addfile',
function(KEYS, ARGV)
    local sta = redis.call('XADD', KEYS[1], '*', KEYS[2], KEYS[3])
    -- local sta1 = redis.call('HSET',KEYS[4],KEYS[3],KEYS[5])
    return sta
end
)
-- myhash, IN, FNAME, fname (addfile)
redis.register_function('reading',
function(KEYS, ARGV)
    local sta = redis.call('HEXISTS',KEYS[1],KEYS[2])
    if sta == 0 then
        redis.call('HSET',KEYS[1],KEYS[2],KEYS[3])
    end
    return sta
end
)