package Illumination.operators;

import Illumination.function.Redises.OccRedisMapFunction;
import Illumination.function.Redises.OpeningApertureRedisMapFunction;
import Illumination.function.Redises.RedisSourceFunction;
import Illumination.function.Sink.RedisSinkFunction;
import Illumination.function.Sink.TableSinkFunction;
import Illumination.models.ExternalTask;
import Illumination.models.orgins.OccCubeModels;
import Illumination.models.orgins.OpeningApertureCubeModels;
import Illumination.models.outputs.StrategyAbnormalRecord;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.TimeUnit;

public class OnlySensorOperator {

    private static String taskId;
    private static String operatorName;

    //数据源参数
    private static String redisUrl;
    private static String redisPassword;
    private static int redisDb;
    private static String kaiduKey;
    private static String renganKey;

    private static int projectId;

    //低碳计算参数
    private static String cubeHost;
    private static String cubeId;
    private static List<String> sensorKeys;
    private static Map<String, Double> sensorPower;
    private static double carbonEmissionFactor;

    //报警参数
    private static String receiverJsonStr;
    private static String templateId;
    private static String warningHost;

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
        DataStream<OpeningApertureCubeModels> openingApertureDS = env
                .addSource(new RedisSourceFunction(redisUrl, redisPassword, redisDb, kaiduKey, projectId, logger))
                .name(taskId + "openingAperture")
                .flatMap(new OpeningApertureRedisMapFunction(logger, sensorKeys))
                .assignTimestampsAndWatermarks(WatermarkStrategy.<OpeningApertureCubeModels>forMonotonousTimestamps().withTimestampAssigner((e, t) -> e.Time.getTime()));
        DataStream<OccCubeModels> occDS = env
                .addSource(new RedisSourceFunction(redisUrl, redisPassword, redisDb, renganKey, projectId, logger))
                .name(taskId + "occ")
                .flatMap(new OccRedisMapFunction(logger, sensorKeys))
                .assignTimestampsAndWatermarks(WatermarkStrategy.<OccCubeModels>forMonotonousTimestamps().withTimestampAssigner((e, t) -> e.Time.getTime()));

        DataStream<StrategyAbnormalRecord> recordDataStream = occDS
                .connect(openingApertureDS)
                .keyBy(x -> x.Key, x -> x.Key)
                .flatMap(new OnlySensorCalculator(taskId, operatorName, sensorPower, carbonEmissionFactor));

        recordDataStream.addSink(new RedisSinkFunction(redisUrl, redisPassword, redisDb, cubeId, projectId, logger));
        recordDataStream.addSink(new TableSinkFunction(
                cubeHost,
                cubeId,
                String.valueOf(projectId),
                warningHost,
                receiverJsonStr,
                templateId,
                logger
        ));
        try {
            env.execute("Illumination-analysis-" + parameters.TaskId);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static void GetParameters(ExternalTask parameters) throws Exception {
        try {
            taskId = parameters.TaskId;
            redisUrl = System.getenv("REDIS");
            redisPassword = System.getenv("REDIS_PASSWORD");
            redisDb = parameters.OperatorParameter.getInteger("RedisDb");
            kaiduKey = parameters.OperatorParameter.getString("OpeningAperture");
            renganKey = parameters.OperatorParameter.getString("OccupancyCubeId");
            projectId = parameters.ProjectId;
            cubeHost = System.getenv("WEB_URL");
            cubeId = parameters.OperatorParameter.getString("CubeId");
            sensorKeys = parameters.OpeningApertureList.toJavaList(String.class);
            sensorPower = parameters.SensorPower;
            carbonEmissionFactor = parameters.CarbonEmission.getDouble("Factor");
            operatorName = parameters.Operator.name();
            receiverJsonStr = parameters.Warning.getJSONArray("Receivers").toJSONString();
            templateId = parameters.Warning.getString("TemplateId");
            warningHost = parameters.Warning.getString("Host");
        } catch (Exception e) {
            throw new Exception("获取数据源参数失败：" + e.getMessage());
        }

    }
}
