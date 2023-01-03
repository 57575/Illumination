package Illumination.models.orgins;


import com.alibaba.fastjson.JSONObject;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

public class OccCubeModels {
    public boolean Occupancy;
    public Date Time;
    public String Key;
    public double Lux;
    public int ProjectId;

    /**
     * 将redis的数据结构转换
     */
    public void Import(JSONObject entity) {
        this.Key = entity.getString("sensor_4");
        this.ProjectId = entity.getInteger("_projectId");

        Object lux = entity.get("lux");
        if (lux instanceof Number) {
            this.Lux = ((Number) lux).doubleValue();
        }

        Object occ = entity.get("occupancy");
        if (occ instanceof Boolean) {
            this.Occupancy = entity.getBooleanValue("occupancy");
        }

        String timeStr = entity.getString("time");
        try {
            SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
            this.Time = sdf.parse(timeStr);
        } catch (ParseException e) {
            e.printStackTrace();
        }
    }

    @Override
    public String toString() {
        return com.alibaba.fastjson.JSONObject.toJSONString(this);
    }
}
