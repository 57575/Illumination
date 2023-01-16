package Illumination.models;

import Illumination.enums.Operators;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;

import java.util.Map;

/**
 * datapipeline中的ExternalTask parametersJson参数
 */
public class ExternalTask {
    public String TaskId;
    public int ProjectId;
    public String KafkaServer;
    public String OpeningAperture;
    public String OCCSensor;
    public long TimeStamp;
    public Operators Operator;
    public JSONObject OperatorParameter;
    public JSONObject Warning;
    public JSONArray OpeningApertureList;
    public Map<String, Double> SensorPower;
    public JSONObject CarbonEmission;
    public String FlinkId;
    public String FlinkSecret;

    public void SetSensorPower(Map<String, Double> map) {
        this.SensorPower = map;
    }
}
