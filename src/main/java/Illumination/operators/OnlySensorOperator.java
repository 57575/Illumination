package Illumination.operators;

import Illumination.function.Redises.OccRedisMapFunction;
import Illumination.function.Redises.OpeningApertureRedisMapFunction;
import Illumination.function.Redises.RedisSourceFunction;
import Illumination.models.ExternalTask;
import Illumination.models.WindowedIllumination;
import Illumination.models.orgins.OccCubeModels;
import Illumination.models.orgins.OpeningApertureCubeModels;
import Illumination.models.outputs.StrategyAbnormalRecord;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.CoGroupFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.TimeUnit;

public class OnlySensorOperator {

    private static String taskId;

    private static String redisUrl;
    private static String redisPassword;
    private static int redisDb;

    private static String kaiduKey;
    private static String renganKey;

    private static int projectId;

    private static String cubeId;

    private static List<String> sensorKeys;

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
                .assignTimestampsAndWatermarks(WatermarkStrategy.<OpeningApertureCubeModels>forBoundedOutOfOrderness(Duration.ofSeconds(5)).withTimestampAssigner((e, t) -> e.Time.getTime()));

        DataStream<OccCubeModels> occDS = env
                .addSource(new RedisSourceFunction(redisUrl, redisPassword, redisDb, renganKey, projectId, logger))
                .name(taskId + "occ")
                .flatMap(new OccRedisMapFunction(logger, sensorKeys))
                .assignTimestampsAndWatermarks(WatermarkStrategy.<OccCubeModels>forBoundedOutOfOrderness(Duration.ofSeconds(5)).withTimestampAssigner((e, t) -> e.Time.getTime()));

        Map<String, StrategyAbnormalRecord> unfinishedRecords = new HashMap<>();

        //合并流
        DataStream<WindowedIllumination> waitCalculateStream = openingApertureDS
                .coGroup(occDS)
                .where(new KeySelector<OpeningApertureCubeModels, String>() {
                    @Override
                    public String getKey(OpeningApertureCubeModels openingApertureCubeModels) throws Exception {
                        return openingApertureCubeModels.Key;
                    }
                })
                .equalTo(new KeySelector<OccCubeModels, String>() {
                    @Override
                    public String getKey(OccCubeModels occCubeModels) throws Exception {
                        return occCubeModels.Key;
                    }
                })
                .window(TumblingEventTimeWindows.of(Time.minutes(5)))
                .allowedLateness(Time.seconds(10))
                .apply(new CoGroupFunction<OpeningApertureCubeModels, OccCubeModels, WindowedIllumination>() {
                    @Override
                    public void coGroup(Iterable<OpeningApertureCubeModels> kaidus, Iterable<OccCubeModels> rengans, Collector<WindowedIllumination> collector) throws Exception {
                        WindowedIllumination item = new WindowedIllumination();
                        String key = "";
                        if (kaidus.iterator().hasNext()) {
                            OpeningApertureCubeModels a = kaidus.iterator().next();
                            key = a.Key;
                        }
                        if (rengans.iterator().hasNext()) {
                            key = rengans.iterator().next().Key;
                        }
                        item.SetName(key);
                        boolean waitClose = false;
                        if (unfinishedRecords.containsKey(key)) {
                            waitClose = true;
                        }
                        for (OpeningApertureCubeModels kaidu : kaidus) {
                            //当有待结束的事件时，第一个关闭值去结束事件
                            if (waitClose && (kaidu.OpeningAperture == 0)) {
                                unfinishedRecords.get(key).SetFinish(kaidu);
                                waitClose = false;
                            }
                            item.SetOpeningApertureValue(kaidu.OpeningAperture);
                            item.SetTimeStamp(kaidu.Time.getTime());
                        }
                        for (OccCubeModels rengan : rengans) {
                            item.SetSensorLUXValue(rengan.Lux);
                            item.SetSensorOCCValue(rengan.Occupancy);
                            item.SetTimeStamp(rengan.Time.getTime());
                        }
                        collector.collect(item);
                    }
                });

        Map<String, Deque<WindowedIllumination>> dataDic = new HashMap<>();
        DataStream<WindowedIllumination> outputStream = waitCalculateStream
                .map(new MapFunction<WindowedIllumination, WindowedIllumination>() {
                    @Override
                    public WindowedIllumination map(WindowedIllumination item) throws Exception {
                        if (dataDic.containsKey(item.Name)) {
                            Deque<WindowedIllumination> deque = dataDic.get(item.Name);
                            OnlySensorCalculator.Calculate(deque, item);
                        } else {
                            Deque<WindowedIllumination> deque = new ArrayDeque<>();
                            OnlySensorCalculator.Calculate(deque, item);
                            dataDic.put(item.Name, deque);
                        }
                        return item;
                    }
                });

        outputStream.print();

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
            renganKey = parameters.OperatorParameter.getString("OCCSensor");
            projectId = parameters.OperatorParameter.getInteger("ProjectId");
            cubeId = parameters.OperatorParameter.getString("CubeId");
            sensorKeys = parameters.OpeningApertureList.toJavaList(String.class);
        } catch (Exception e) {
            throw new Exception("获取数据源参数失败：" + e.getMessage());
        }

    }
}
