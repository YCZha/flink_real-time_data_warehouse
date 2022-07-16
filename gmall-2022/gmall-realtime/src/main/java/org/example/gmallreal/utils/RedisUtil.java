package org.example.gmallreal.utils;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

//通过JedisPool连接池获取Jedis连接
public class RedisUtil {
    private static JedisPool jedisPool = null;

    public static Jedis getJedis(){
        if(jedisPool == null){
            JedisPoolConfig jedisPoolConfig = new JedisPoolConfig();
            jedisPoolConfig.setMaxTotal(100); //最大可用连接数
            jedisPoolConfig.setBlockWhenExhausted(true); //连接耗尽是否等待
            jedisPoolConfig.setMaxWaitMillis(2000); //等待时间
            jedisPoolConfig.setMaxIdle(5); //最大闲置连接数
            jedisPoolConfig.setMinIdle(5); //最小闲置连接数
            jedisPoolConfig.setTestOnBorrow(true); //取连接的时候进行一下测试 ping pong

            jedisPool = new JedisPool(jedisPoolConfig,"hadoop102",6379,10000);
            System.out.println("redis连接成功！");
        }
        return jedisPool.getResource();
    }

    public static void main(String[] args) {
        Jedis jedis = getJedis();
        System.out.println(jedis.ping());
    }
}
