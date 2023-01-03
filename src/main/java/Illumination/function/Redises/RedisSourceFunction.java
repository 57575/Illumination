package Illumination.function.Redises;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.slf4j.Logger;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPubSub;

import java.util.Map;

public class RedisSourceFunction extends RichSourceFunction<String> {

    private final String redisUrl;
    private final String redisPassword;
    private final int redisDb;
    private Logger log;

    private boolean isRunning = true;
    private Jedis jedis = null;
    private final String redisKey;
    private final String channel;

    public RedisSourceFunction(String redisUrl, String redisPassword, int redisDb, String redisKey, int projectId, Logger logger) {
        this.redisUrl = redisUrl;
        this.redisPassword = redisPassword;
        this.redisDb = redisDb;
        this.log = logger;
        this.redisKey = redisKey;
        this.channel = redisDb + "-" + "RT-" + redisKey + "-" + projectId + "-*";
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
    }

    @Override
    public void run(SourceContext<String> sourceContext) throws Exception {
        log.info("RedisSourceFunction Run");
        if (jedis == null) {
            jedis = new Jedis(redisUrl, 6379);
            jedis.auth(redisPassword);
            jedis.select(redisDb);
        }
        Map<String, String> initialData = jedis.hgetAll(redisKey);
        for (String initialD : initialData.values()) {
            sourceContext.collect(initialD);
        }

        JedisPubSub jedisPubSub = new JedisPubSub() {
            @Override
            public void onPMessage(String pattern, String channel, String message) {
                sourceContext.collect(message);
            }
        };
        jedis.psubscribe(jedisPubSub, channel);
    }

    @Override
    public void cancel() {
        if (jedis != null) {
            jedis.close();
            jedis = null;
        }
    }

}
