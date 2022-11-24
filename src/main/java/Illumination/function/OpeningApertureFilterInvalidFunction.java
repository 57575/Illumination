package Illumination.function;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.functions.FilterFunction;
import org.slf4j.Logger;

public class OpeningApertureFilterInvalidFunction implements FilterFunction<JSONObject> {

    private final Logger LOG;

    public OpeningApertureFilterInvalidFunction(Logger log) {
        LOG = log;
    }


    @Override
    public boolean filter(JSONObject jsonObject) throws Exception {
        if ((!jsonObject.containsKey("value") || !jsonObject.getJSONObject("value").containsKey("switch_4"))) {
            LOG.info("开度数据源不包含键值sensor_4,丢弃,数据:" + JSON.toJSONString(jsonObject));
            return false;
        } else {
            return true;
        }
    }
}
