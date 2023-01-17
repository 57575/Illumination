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
    private static long programStartTime;

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
                .keyBy(occ -> occ.Key, open -> open.Key)
                .flatMap(new OnlySensorCalculator(programStartTime, operatorName, sensorPower, carbonEmissionFactor));

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


//        openingApertureDS.flatMap(new OpeningApertureMapFunction(unfinishedRecords, last20minuteOcc, lastOpeningAperture));
//
//        occDS
//                .keyBy(x -> x.Key)
//                .window(TumblingEventTimeWindows.of(Time.minutes(1)))
//                .process(new OccupancyWindowFunction(unfinishedRecords, last20minuteOcc, lastOpeningAperture, OperatorName));


        //合并流
//        DataStream<StrategyAbnormalRecord> waitCalculateStream = openingApertureDS
//                .coGroup(occDS)
//                .where(new KeySelector<OpeningApertureCubeModels, String>() {
//                    @Override
//                    public String getKey(OpeningApertureCubeModels openingApertureCubeModels) throws Exception {
//                        return openingApertureCubeModels.Key;
//                    }
//                })
//                .equalTo(new KeySelector<OccCubeModels, String>() {
//                    @Override
//                    public String getKey(OccCubeModels occCubeModels) throws Exception {
//                        return occCubeModels.Key;
//                    }
//                })
//                .window(TumblingEventTimeWindows.of(Time.minutes(1)))
//                .apply(new CoGroupFunction<OpeningApertureCubeModels, OccCubeModels, StrategyAbnormalRecord>() {
//                    @Override
//                    public void coGroup(Iterable<OpeningApertureCubeModels> kaidus, Iterable<OccCubeModels> rengans, Collector<StrategyAbnormalRecord> collector) throws Exception {
//                        System.out.println("开始聚合窗口,时间:" + System.currentTimeMillis());
//
//                        String key = "";
//                        boolean hasKaidu = kaidus.iterator().hasNext();
//                        boolean hasRengan = rengans.iterator().hasNext();
//
//                        if (hasKaidu) {
//                            key = kaidus.iterator().next().Key;
//                        }
//        if (hasRengan) {
//            //寻找key
//            key = rengans.iterator().next().Key;
//            //聚合本时间窗的人感数据，并添加至聚合Map
//            OccCubeModels reducedRengan = rengans.iterator().next();
//            for (OccCubeModels rengan : rengans) {
//                reducedRengan.Reduce(rengan);
//                System.out.println(rengan.toString());
//            }
//            WindowedOcc windowedOcc = new WindowedOcc();
//            if (last20minuteOcc.containsKey(key)) {
//                windowedOcc = last20minuteOcc.get(key);
//            }
//            windowedOcc.AddNewItem(reducedRengan);
//            last20minuteOcc.put(key, windowedOcc);
//            System.out.println(windowedOcc.toString());
//            System.out.println(last20minuteOcc.toString());
//        }
//                        //有前20分钟的任意人感数据，才判断是否为异常事件
//                        if (last20minuteOcc.containsKey(key)) {
//                            WindowedOcc last20Occ = last20minuteOcc.get(key);
//                            for (OpeningApertureCubeModels kaidu : kaidus) {
//                                System.out.println(kaidu.toString());
//
//                                boolean waitClose = unfinishedRecords.containsKey(key);
//                                //开度为开启，包括了过去20分钟的人感，过去20分钟人感皆为假,没有等待关闭的事件，此时应当发出报警
//                                if (kaidu.OpeningAperture > 0.5 && last20Occ.Duration >= 20 * 60 * 1000 && (!last20Occ.Occupancy) && (!waitClose)) {
//                                    JSONObject originalData = new JSONObject();
//                                    originalData.put("OpeningAperture", kaidu);
//                                    originalData.put("Occupancy", last20Occ);
//                                    StrategyAbnormalRecord record = new StrategyAbnormalRecord(
//                                            kaidu.Time.getTime(),
//                                            "无人值守",
//                                            "照明系统",
//                                            kaidu.Key,
//                                            OperatorName,
//                                            "",
//                                            originalData);
//                                    unfinishedRecords.put(key, record);
//                                    PostWarning.PostWarningMessage(parameters.Warning, record, logger);
//                                    System.out.println("产生报警事件");
//                                    System.out.println(record.toString());
//                                }
//                                //开度为关闭，且有待关闭的事件
//                                if (kaidu.OpeningAperture < 0.5 && waitClose) {
//                                    StrategyAbnormalRecord record = unfinishedRecords.get(key);
//                                    record.SetFinish(kaidu.Time.getTime());
//                                    record.SetCarbon(1, "kg");
//                                    collector.collect(record);
//                                    unfinishedRecords.remove(key);
//                                    System.out.println("关闭报警事件");
//                                    System.out.println(record.toString());
//                                }
//                            }
//                        }
//                        System.out.println("结束聚合窗口,时间:" + System.currentTimeMillis());
//                    }
//                });
//
//        waitCalculateStream.print();

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
            programStartTime = System.currentTimeMillis();
            receiverJsonStr = parameters.Warning.getJSONArray("Receivers").toJSONString();
            templateId = parameters.Warning.getString("TemplateId");
            warningHost = parameters.Warning.getString("Host");
        } catch (Exception e) {
            throw new Exception("获取数据源参数失败：" + e.getMessage());
        }

    }
}
