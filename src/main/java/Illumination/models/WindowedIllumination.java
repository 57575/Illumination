package Illumination.models;

import com.alibaba.fastjson.JSONObject;

public class WindowedIllumination {
    public String Name;
    public long TimeStamp;
    public String TimeStr;
    public boolean IsWarning;
    /**
     * 应当包括
     * <p>OpeningAperture  开度,double;</p>
     * <p>SensorOCC        人感,boolean;</p>
     * <p>SensorLUX        照度,double;</p>
     */
    public JSONObject OriginalData;
    public String Message;

    public long FirstOpenTime;
    public String Strategy;

    public WindowedIllumination() {
        Name = "";
        TimeStamp = 0;
        TimeStr = "";
        IsWarning = false;
        OriginalData = new JSONObject();
        Message = "";
        FirstOpenTime = 0;
        Strategy = "";
    }

    /**
     * 开度值，总是采用最后一个数据
     */
    public void SetOpeningApertureValue(double value) {
        OriginalData.put("OpeningAperture", value);
    }

    /**
     * 聚合人感数据，false值才会被新的值覆盖
     *
     * @param value
     */
    public void SetSensorOCCValue(boolean value) {
        if (!OriginalData.getBooleanValue("SensorOCC")) {
            OriginalData.put("SensorOCC", value);
        }
    }

    /**
     * 计算照度的均值并赋值
     */
    public void SetSensorLUXValue(double value) {
        if (OriginalData.containsKey("SensorLUX")) {
            double last = OriginalData.getDouble("SensorLUX");
            OriginalData.put("SensorLUX", (value + last) / 2);
        } else {
            OriginalData.put("SensorLUX", value);
        }
    }

    public void SetName(String name) {
        if (Name == null || Name.isEmpty()) {
            Name = name;
        }
    }

    public void SetTimeStamp(long timeStamp) {
        if (timeStamp > TimeStamp) {
            TimeStamp = timeStamp;
        }
    }

    public void SetIsWarning(boolean value) {
        IsWarning = value;
    }

    public void AppendMessage(String m) {
        Message = Message.isEmpty() ? m : Message.concat("|").concat(m);
    }

    public void SetOpenAndTime(double value, long time) {
        OriginalData.put("OpeningAperture", value);
    }

    public void setFirstOpenTime(long firstOpenTime) {
        FirstOpenTime = firstOpenTime;
    }

    public void setStrategy(String strategy) {
        Strategy = strategy;
    }

    @Override
    public String toString() {
        return com.alibaba.fastjson.JSONObject.toJSONString(this);
    }

}
