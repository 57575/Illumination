package Illumination.utils;

import org.slf4j.Logger;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPubSub;

public class RedisSubscriberThread extends Thread {
    private final JedisPool jedisPool;
    private final JedisPubSub jedisSubscribe;

    private final String channel;
    private final String redisPassword;
    private final Logger log;

    public RedisSubscriberThread(JedisPool jedisPool, String redisPassword, String channel, JedisPubSub jedisSubscribe, Logger log) {
        super("Subscriber");
        this.jedisPool = jedisPool;
        this.jedisSubscribe = jedisSubscribe;
        this.channel = channel;
        this.redisPassword = redisPassword;
        this.log = log;

    }

    @Override
    public void run() {
        // 注意：subscribe是一個阻塞的方法，在取消訂閱該頻道前，thread會一直阻塞在這，無法執行會後續的Code
        log.info(String.format("subscribe redis, channel %s, thread will be blocked", channel));

        Jedis jedis = null;

        try {
            jedis = jedisPool.getResource();   /* 取出一个連線*/
            jedis.auth(redisPassword);
            jedis.psubscribe(jedisSubscribe, channel);    //通過subscribe 的api去訂閱，傳入參數為訂閱者和頻道名
        } catch (Exception e) {
            log.error(String.format("subscribe channel error, %s", e));
        } finally {
            if (jedis != null) {
                jedis.close();
            }
        }
    }
}
