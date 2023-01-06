package Illumination.models;

import Illumination.enums.Operators;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;

/**
 * datapipeline中的ExternalTask parametersJson参数
 */
public class ExternalTask {
    public String TaskId;
    public String KafkaServer;
    public String OpeningAperture;
    public String OCCSensor;
    public long TimeStamp;
    public Operators Operator;
    public JSONObject OperatorParameter;
    public JSONObject Warning;
    public JSONArray OpeningApertureList;
}
