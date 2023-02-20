package Illumination.models.outputs;

import com.alibaba.fastjson.JSONObject;

import java.io.Serializable;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.TimeZone;

public class StrategyAbnormalRecord implements Serializable {
    private static final long serialVersionUID = -4796066605098841564L;
    public String Time;
    public long StartTimeStamp;
    public boolean IsFinish;
    public long FinishTimeStamp;
    /**
     * 事件类型
     */
    public String Type;
    public String System;
    public String SensorKey;
    public String Strategy;
    public double CarbonEmissions;
    public String CarbonUnit;
    public String Message;
    public long Duration;
    public String Measure;
    public JSONObject OriginalData;

    /**
     * @param timeStamp 事件开始时间
     * @param type      事件类型
     * @param system    事件所属系统类型
     * @param strategy  所使用的策略
     */
    public StrategyAbnormalRecord(long timeStamp, String type, String system, String sensorKey, String strategy, String message, JSONObject originalData) {
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        this.Time = sdf.format(timeStamp);
        this.StartTimeStamp = timeStamp;
        this.IsFinish = false;
        this.Type = type;
        this.System = system;
        this.SensorKey = sensorKey;
        this.Strategy = strategy;
        this.Message = message;
        this.OriginalData = originalData;
        this.Measure = "关闭";
    }

    public void SetCarbon(double carbonEmissions, String carbonUnit) {
        this.CarbonEmissions = carbonEmissions;
        this.CarbonUnit = carbonUnit;
    }

    public void SetFinish(long timeStamp) {
        this.FinishTimeStamp = timeStamp;
        this.IsFinish = true;
        this.Duration = (timeStamp - this.StartTimeStamp) / 1000;
    }

    public long GetDuration() {
        return this.Duration;
    }

    @Override
    public String toString() {
        return com.alibaba.fastjson.JSONObject.toJSONString(this);
    }

    public String toCubeJsonStr() {
        JSONObject result = toCubeJSONObject();
        return com.alibaba.fastjson.JSONObject.toJSONString(result);
    }

    public JSONObject toCubeJSONObject() {
        JSONObject result = new JSONObject();
        result.put("_key", this.SensorKey);
        Calendar calendar = Calendar.getInstance(TimeZone.getTimeZone("Asia/Shanghai"));
        calendar.setTimeInMillis(this.StartTimeStamp);
        result.put("time", this.Time);
        result.put("time_year", calendar.get(Calendar.YEAR));
        int month = calendar.get(Calendar.MONTH);
        month += 1;
        result.put("time_month", month);
        result.put("time_day", calendar.get(Calendar.DAY_OF_MONTH));
        result.put("time_hour", calendar.get(Calendar.HOUR_OF_DAY));
        result.put("time_minute", calendar.get(Calendar.MINUTE));
        result.put("time_second", calendar.get(Calendar.SECOND));
        result.put("time_week", calendar.get(Calendar.WEEK_OF_YEAR));
        int weekday = calendar.get(Calendar.DAY_OF_WEEK);
        weekday = weekday == 0 ? 7 : weekday - 1;
        result.put("time_weekday", weekday);
        result.put("type", this.Type);
        result.put("system", this.System);
        result.put("sensor_key", this.SensorKey);
        result.put("strategy", this.Strategy);
        result.put("carbon_emissions", this.CarbonEmissions);
        result.put("origin_data", com.alibaba.fastjson.JSONObject.toJSONString(this.OriginalData));
        result.put("message", this.Message);
        result.put("duration", this.Duration);
        result.put("measure", this.Measure);
        return result;
    }
}
