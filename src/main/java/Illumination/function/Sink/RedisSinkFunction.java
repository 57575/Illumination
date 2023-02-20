package Illumination.function.Sink;

import Illumination.models.outputs.StrategyAbnormalRecord;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.slf4j.Logger;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.Pipeline;

public class RedisSinkFunction extends RichSinkFunction<StrategyAbnormalRecord> {

    private static final long serialVersionUID = -8429053747787023127L;
    private final String redisUrl;
    private final String redisPassword;
    private final int redisDb;
    private Logger log;
    private transient Pipeline pipeline;
    private final String redisKey;
    private final String channel;

    public RedisSinkFunction(String redisUrl, String redisPassword, int redisDb, String redisKey, int projectId, Logger logger) {
        this.redisUrl = redisUrl;
        this.redisPassword = redisPassword;
        this.redisDb = redisDb;
        this.log = logger;
        this.redisKey = redisKey;
        this.channel = redisDb + "-RT-" + redisKey + "-" + projectId + "-records";
    }

    @Override
    public void invoke(StrategyAbnormalRecord item, Context context) throws Exception {
        JSONObject first = new JSONObject();
        first.put("key", item.SensorKey);
        first.put("sourceId", redisKey);
        first.put("value", item.toCubeJSONObject());
        first.put("timestamp", item.StartTimeStamp);
        JSONObject result = new JSONObject();
        result.put("item", first);
        result.put("key", item.SensorKey);
        result.put("time", item.Time);
        pipeline.publish(channel, result.toJSONString());
        pipeline.sync();
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        if (pipeline == null) {
            Jedis redis = new Jedis(redisUrl, 6379);
            redis.auth(redisPassword);
            redis.select(redisDb);
            pipeline = redis.pipelined();
        }
    }

    @Override
    public void close() throws Exception {
        super.close();
        pipeline.close();
        pipeline = null;
    }
}
