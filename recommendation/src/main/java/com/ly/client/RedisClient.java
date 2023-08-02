package com.ly.client;

import com.ly.util.Property;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

import java.util.ArrayList;
import java.util.List;

public class RedisClient {
    public static Jedis jedis;

    //静态代码块初始化 redis
    static {
//        JedisPoolConfig poolConfig = new JedisPoolConfig();
//        poolConfig.setMaxTotal(10);
//        poolConfig.setMaxWaitMillis(1000);
//        JedisPool jedisPool = new JedisPool(poolConfig, Property.getStrValue("redis.host"), Property.getIntegerValue("redis.port"));
//        jedis = jedisPool.getResource();
        jedis = new Jedis(Property.getStrValue("redis.host"), Property.getIntegerValue("redis.port"),6000,6000);
        jedis.auth(Property.getStrValue("redis.password"));
        jedis.select(Property.getIntegerValue("redis.db"));

    }

    public static String getData(String s) {
        return jedis.get(s);
    }

    public static boolean putData(String key, String value) {
        return jedis.append(key, value) != null;
    }

    public static boolean rpush(String key, List<String> value) {
        for(int i = 0; i < value.size(); i++) {
            jedis.rpush(key, value.get(i));
        }
        return true;
    }

    public static List<String> lrange(String key) {
        return jedis.lrange(key, 0, -1);
    }
}
