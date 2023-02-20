package Illumination.function;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.io.IOException;
import java.time.Duration;

public class KafkaSourceFunction {
    public static DataStream<JSONObject> GetSource(StreamExecutionEnvironment env, String kafkaServer, String topic, String group, String watermarkProName) {
        KafkaSource<JSONObject> sourceBuilder = KafkaSource.<JSONObject>builder()
                .setBootstrapServers(kafkaServer)
                .setTopics(topic)
                .setGroupId(group)
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new DeserializationSchema<JSONObject>() {
                    @Override
                    public JSONObject deserialize(byte[] bytes) throws IOException {
                        return JSON.parseObject((new String(bytes)));
                    }

                    @Override
                    public boolean isEndOfStream(JSONObject jsonObject) {
                        return false;
                    }

                    @Override
                    public TypeInformation<JSONObject> getProducedType() {
                        return TypeInformation.of(JSONObject.class);
                    }
                })
                .build();
        return env.fromSource(sourceBuilder, WatermarkStrategy.noWatermarks(), "kafka-" + topic)
                .assignTimestampsAndWatermarks(WatermarkStrategy.<JSONObject>forBoundedOutOfOrderness(Duration.ofSeconds(5)).withTimestampAssigner((e, t) -> e.getLong(watermarkProName)));
    }
}
