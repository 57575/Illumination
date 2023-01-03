package Illumination;

import Illumination.models.ExternalTask;
import Illumination.operators.OnlySensorOperator;
import Illumination.operators.SensorAndTimeOperator;
import Illumination.utils.PostMessage;
import Illumination.utils.RedisSubscriber;
import Illumination.utils.RedisSubscriberThread;
import com.alibaba.fastjson.JSONObject;
import kong.unirest.HttpResponse;
import kong.unirest.Unirest;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;
import redis.clients.jedis.JedisPubSub;


public class Job {

    public static void main(String[] args) throws Exception {
        ExternalTask parameters = GetParameters(args);
        switch (parameters.Operator) {
            case onlyTimeSchedule:
                break;
            case onlySensorSchedule:
                OnlySensorOperator.Run(parameters);
                break;
            case sensorAndTime:
                SensorAndTimeOperator.Run(parameters);
                break;
            default:
                break;
        }
    }

    private static ExternalTask GetParameters(String[] args) throws Exception {
        if (args.length < 1) throw new Exception("输入参数数量不足");
        String taskId = args[0];
        Unirest.config().addDefaultHeader("flinkId", System.getenv("FLINK_ID"));
        Unirest.config().addDefaultHeader("flinkSecret", System.getenv("FLINK_SECRET"));
        String url = System.getenv("WEB_URL") + "/api/datapipeline/projects/0/externaltask/" + taskId;
        HttpResponse<String> result = Unirest.get(url).asString();
        if (!result.isSuccess()) throw new Exception("无法查询任务详情,status:" + result.getStatus());
        JSONObject task = JSONObject.parseObject(result.getBody());
        if (task == null) throw new Exception("无法查询任务详情,body:" + result.getBody());
        ExternalTask parameters = task.getJSONObject("parametersJson").toJavaObject(ExternalTask.class);
        parameters.TaskId = task.getString("id");

        return parameters;
    }


}
