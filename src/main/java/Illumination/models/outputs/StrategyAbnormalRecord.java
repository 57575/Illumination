package Illumination.models.outputs;

import Illumination.models.orgins.OpeningApertureCubeModels;
import com.alibaba.fastjson.JSONObject;

public class StrategyAbnormalRecord {
    public String Time;
    public long TimeStamp;
    public long FinishTimeStamp;
    public String Type;
    public String System;
    public String SensorKey;
    public String Strategy;
    public double CarbonEmissions;
    public String CarbonUnit;
    public String Message;
    public int Duration;
    public String Measure;
    public JSONObject OriginalData;

    /**
     * 初始化函数
     */
    public StrategyAbnormalRecord() {
        Time = "";
        TimeStamp = 0;
        Type = "";
        System = "照明系统";
        SensorKey = "";
        Strategy = "";
        CarbonEmissions = 0;
        CarbonUnit = "kg";
        Message = "";
        Duration = 0;
        Measure = "";
        OriginalData = new JSONObject();
    }


    public void SetFinish(OpeningApertureCubeModels finish) {

    }

    @Override
    public String toString() {
        return com.alibaba.fastjson.JSONObject.toJSONString(this);
    }
}
