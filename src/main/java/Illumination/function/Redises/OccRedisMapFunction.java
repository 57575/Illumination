package Illumination.function.Redises;

import Illumination.models.orgins.OccCubeModels;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;

import java.util.List;

public class OccRedisMapFunction implements FlatMapFunction<String, OccCubeModels> {

    private static final long serialVersionUID = -878474245596830943L;
    private org.slf4j.Logger log;
    private List<String> keys;

    public OccRedisMapFunction(Logger log, List<String> keys) {
        this.log = log;
        this.keys = keys;
    }

    @Override
    public void flatMap(String s, Collector<OccCubeModels> collector) throws Exception {
        JSONObject origin = com.alibaba.fastjson.JSON.parseObject(s);
        if (!origin.containsKey("item")) {
            System.out.println("人感redis数据错误;" + s);
            log.info("人感redis数据错误;" + s);
            return;
        }
        JSONObject item = origin.getJSONObject("item");
        if (!item.containsKey("value")) {
            System.out.println("人感redis数据不包含值;" + s);
            log.info("人感redis数据不包含值;" + s);
            return;
        }
        JSONObject value = item.getJSONObject("value");
        if (!value.containsKey("sensor_4")) {
            log.info("开度数据源不包含键值sensor_4,丢弃,数据:" + JSON.toJSONString(value));
            return;
        }
        String key = value.getString("sensor_4");
        if (!this.keys.contains(key)) {
            return;
        }
        OccCubeModels result = new OccCubeModels();
        result.Import(value);
        collector.collect(result);
    }
}
