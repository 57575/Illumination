package Illumination.operators;

import Illumination.function.OCCFilterInvalidFunction;
import Illumination.function.OpeningApertureFilterInvalidFunction;
import Illumination.function.SourceFunction;
import Illumination.models.ExternalTask;
import Illumination.models.WindowedIllumination;

import Illumination.utils.PostMessage;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.serializer.SerializerFeature;
import org.apache.flink.api.common.functions.CoGroupFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.TimeUnit;

public class SensorAndTimeOperator {
    private static String kafkaServer;
    private static String kaiduTopic;
    private static String renganTopic;
    private static String renganGroup;
    private static String kaiduGroup;
    private static String sinkTopic;

    public static void Run(ExternalTask parameter) {
        Logger logger = LoggerFactory.getLogger("Illumination-logs-" + parameter.TaskId);

        //注册参数
        try {
            GetParameters(parameter);
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
        DataStream<JSONObject> openingApertureDS = SourceFunction.GetSource(env, kafkaServer, kaiduTopic, kaiduGroup, "timestamp")
                .filter(new OpeningApertureFilterInvalidFunction(logger));

        DataStream<JSONObject> occDS = SourceFunction.GetSource(env, kafkaServer, renganTopic, renganGroup, "timestamp")
                .filter(new OCCFilterInvalidFunction(logger));

        //合并流
        DataStream<WindowedIllumination> waitCalculateStream = openingApertureDS
                .coGroup(occDS)
                .where(new KeySelector<JSONObject, Object>() {
                    @Override
                    public Object getKey(JSONObject jsonObject) throws Exception {
                        return jsonObject.getJSONObject("value").getString("switch_4");
                    }
                })
                .equalTo(new KeySelector<JSONObject, Object>() {
                    @Override
                    public Object getKey(JSONObject jsonObject) throws Exception {
                        return jsonObject.getJSONObject("value").getString("sensor_4");
                    }
                })
                .window(TumblingEventTimeWindows.of(Time.minutes(5)))
                .apply(new CoGroupFunction<JSONObject, JSONObject, WindowedIllumination>() {
                    @Override
                    public void coGroup(Iterable<JSONObject> kaidus, Iterable<JSONObject> rengans, Collector<WindowedIllumination> collector) throws Exception {
                        WindowedIllumination item = new WindowedIllumination();
                        for (JSONObject kaidu : kaidus) {
                            if (kaidu.containsKey("value")) {
                                JSONObject kaiduValue = kaidu.getJSONObject("value");
                                item.SetName(kaiduValue.getString("switch_4"));
                                if (kaiduValue.containsKey("value")) {
                                    Object v = kaiduValue.get("value");
                                    if (v instanceof Number) {
                                        item.SetOpeningApertureValue(kaiduValue.getDoubleValue("value"));
                                    }
                                }
                                item.SetTimeStamp(kaidu.getLongValue("timestamp"));
                            } else {
                                System.out.println(kaidu);
                            }
                        }
                        for (JSONObject rengan : rengans) {
                            if (rengan.containsKey("value")) {
                                JSONObject renganValue = rengan.getJSONObject("value");
                                item.SetName(renganValue.getString("sensor_4"));
                                if (renganValue.containsKey("lux")) {
                                    Object lux = renganValue.get("lux");
                                    if (lux instanceof Number) {
                                        item.SetSensorLUXValue(renganValue.getDoubleValue("lux"));
                                    }
                                }
                                if (renganValue.containsKey("occupancy")) {
                                    Object occ = renganValue.get("occupancy");
                                    if (occ instanceof Boolean) {
                                        item.SetSensorOCCValue(renganValue.getBooleanValue("occupancy"));
                                    }
                                }
                                item.SetTimeStamp(rengan.getLongValue("timestamp"));
                            }
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
                            SensorAndTimeCalculator.Calculate(deque, item);
                        } else {
                            Deque<WindowedIllumination> deque = new ArrayDeque<>();
                            SensorAndTimeCalculator.Calculate(deque, item);
                            dataDic.put(item.Name, deque);
                        }
                        return item;
                    }
                });

        outputStream.map(new PostMessage(parameter.Warning, logger));

        Sink(outputStream);

        try {
            env.execute("Illumination-analysis-" + parameter.TaskId);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static void GetParameters(ExternalTask parameters) throws Exception {
        try {
            kafkaServer = parameters.KafkaServer;
            kaiduTopic = "withoutWindow-" + parameters.OpeningAperture;
            renganTopic = "withoutWindow-" + parameters.OCCSensor;
            sinkTopic = "Illumination-analysis-" + parameters.TaskId;
            renganGroup = "Illumination-OpeningAperture" + "-" + parameters.TaskId;
            kaiduGroup = "Illumination-OCCSensor" + "-" + parameters.TaskId;
            SensorAndTimeCalculator.SetTime(
                    parameters.OperatorParameter.getInteger("WorkStartTime"),
                    parameters.OperatorParameter.getInteger("WorkEndTime")
            );
        } catch (Exception e) {
            throw new Exception("获取数据源参数失败：" + e.getMessage());
        }

    }

    private static void Sink(DataStream<WindowedIllumination> dataStream) {
        KafkaSink<String> kafkaSink = KafkaSink.<String>builder()
                .setBootstrapServers(kafkaServer)
                .setRecordSerializer(KafkaRecordSerializationSchema.builder()
                        .setTopic(sinkTopic)
                        .setValueSerializationSchema(new SimpleStringSchema())
                        .build())
                .setDeliverGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
                .build();
        dataStream.map(new MapFunction<WindowedIllumination, String>() {
            @Override
            public String map(WindowedIllumination windowedIllumination) throws Exception {
                return JSON.toJSONString(windowedIllumination, SerializerFeature.PrettyFormat, SerializerFeature.WriteMapNullValue);
            }
        }).sinkTo(kafkaSink);

    }
}
