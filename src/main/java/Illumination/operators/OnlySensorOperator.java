package Illumination.operators;

import Illumination.function.OpeningApertureFilterInvalidFunction;
import Illumination.function.RedisSourceFunction;
import Illumination.function.SourceFunction;
import Illumination.models.ExternalTask;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.TimeZone;
import java.util.concurrent.TimeUnit;

public class OnlySensorOperator {

    private static String kafkaServer;
    private static String renganGroup;
    private static String kaiduGroup;

    private static String taskId;

    private static String redisUrl;
    private static String redisPassword;
    private static int redisDb;

    private static String kaiduKey;
    private static String renganKey;

    private static int projectId;

    private static String cubeId;


    public static void Run(ExternalTask parameters) {
        Logger logger = LoggerFactory.getLogger("Illumination-logs-" + parameters.TaskId);

        //注册参数
        try {
            GetParameters(parameters);
        } catch (Exception e) {
            e.getMessage();
        }

        // 初始化流计算环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        TimeZone.setDefault(TimeZone.getTimeZone("Asia/Shanghai"));
        env.enableCheckpointing(10000, CheckpointingMode.EXACTLY_ONCE);
        CheckpointConfig checkpointConfig = env.getCheckpointConfig();
        checkpointConfig.enableExternalizedCheckpoints(
                CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        checkpointConfig.setMaxConcurrentCheckpoints(1);
        checkpointConfig.setMinPauseBetweenCheckpoints(1000);
        env.setRestartStrategy(
                RestartStrategies.fixedDelayRestart(5, org.apache.flink.api.common.time.Time.of(1, TimeUnit.MINUTES)));
        env.setParallelism(1);

        //获取源
        DataStream<String> openingApertureDS = env
                .addSource(new RedisSourceFunction(redisUrl, redisPassword, redisDb, kaiduKey, logger))
                .name(taskId);

        openingApertureDS.print();
    }

    private static void GetParameters(ExternalTask parameters) throws Exception {
        try {
            taskId = parameters.TaskId;
            redisUrl = System.getenv("REDIS");
            redisPassword = System.getenv("REDIS_PASSWORD");
            redisDb = parameters.OperatorParameter.getInteger("RedisDb");
            kaiduKey = parameters.OperatorParameter.getString("OpeningAperture");
            renganKey = parameters.OperatorParameter.getString("OCCSensor");
            projectId = parameters.OperatorParameter.getInteger("ProjectId");
            cubeId = parameters.OperatorParameter.getString("CubeId");
//            SensorAndTimeCalculator.SetTime(
//                    parameters.OperatorParameter.getInteger("WorkStartTime"),
//                    parameters.OperatorParameter.getInteger("WorkEndTime")
//            );
        } catch (Exception e) {
            throw new Exception("获取数据源参数失败：" + e.getMessage());
        }

    }
}
