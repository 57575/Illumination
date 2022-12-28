package Illumination.function;

import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.slf4j.Logger;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPubSub;
import redis.clients.jedis.Pipeline;

import java.util.Map;
import java.util.Timer;

public class RedisSourceFunction extends RichSourceFunction<String> {

    private final String redisUrl;
    private final String redisPassword;
    private final int redisDb;
    private transient Pipeline pipeline;
    private Logger log;

    private boolean isRunning = true;
    private Jedis jedis = null;
    private final String redisKey;

    public RedisSourceFunction(String redisUrl, String redisPassword, int redisDb, String redisKey, Logger logger) {
        this.redisUrl = redisUrl;
        this.redisPassword = redisPassword;
        this.redisDb = redisDb;
        this.log = logger;
        this.redisKey = redisKey;
    }

    @Override
    public void run(SourceContext<String> sourceContext) throws Exception {
        if (jedis == null) {
            jedis = new Jedis(redisUrl, 6379);
            jedis.auth(redisPassword);
            jedis.select(redisDb);
        }
        Map<String, String> initData = jedis.hgetAll(this.redisKey);
        for (String initD : initData.values()) {
            sourceContext.collect(initD);
        }
    }

    @Override
    public void cancel() {
        isRunning = false;
        while (jedis != null) {
            jedis.close();
            jedis = null;
        }
    }

}
