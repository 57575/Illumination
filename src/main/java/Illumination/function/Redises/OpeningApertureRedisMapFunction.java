package Illumination.function.Redises;

import Illumination.models.orgins.OpeningApertureCubeModels;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;

public class OpeningApertureRedisMapFunction implements FlatMapFunction<String, OpeningApertureCubeModels> {

    private org.slf4j.Logger log;

    public OpeningApertureRedisMapFunction(Logger logger) {
        this.log = logger;
    }

    @Override
    public void flatMap(String s, Collector<OpeningApertureCubeModels> collector) throws Exception {
        JSONObject origin = com.alibaba.fastjson.JSON.parseObject(s);
        if (!origin.containsKey("item")) {
            System.out.println("开度redis数据错误;" + s);
            log.info("开度redis数据错误;" + s);
            return;
        }
        JSONObject item = origin.getJSONObject("item");
        if (!item.containsKey("value")) {
            System.out.println("开度redis数据不包含值;" + s);
            log.info("开度redis数据不包含值;" + s);
            return;
        }
        JSONObject value = item.getJSONObject("value");
        if (!value.containsKey("switch_4")) {
            log.info("开度数据源不包含键值switch_4,丢弃,数据:" + JSON.toJSONString(value));
            return;
        }
        OpeningApertureCubeModels result = new OpeningApertureCubeModels();
        result.Import(value);
        collector.collect(result);
    }
}
