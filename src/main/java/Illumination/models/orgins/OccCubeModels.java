package Illumination.models.orgins;


import com.alibaba.fastjson.JSONObject;

import java.io.Serializable;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

public class OccCubeModels implements Serializable {
    private static final long serialVersionUID = -2981062924379577969L;
    public boolean Occupancy;
    public Date Time;
    public String Key;
    public double Lux;
    public int ProjectId;

    public OccCubeModels() {
    }

    public OccCubeModels(String key, int projectId) {
        this.Key = key;
        this.ProjectId = projectId;
        this.Lux = 0;
        this.Time = new Date(0L);
        this.Occupancy = false;
    }

    public OccCubeModels(String key, int project, boolean occ, double lux, long time) {
        Key = key;
        ProjectId = project;
        Occupancy = occ;
        Lux = lux;
        Time = new Date(time);
    }

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
        if (occ instanceof Number) {
            if ((int) occ == 0) {
                this.Occupancy = false;
            } else {
                this.Occupancy = true;
            }
        }

        String timeStr = entity.getString("time");
        try {
            SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
            this.Time = sdf.parse(timeStr);
        } catch (ParseException e) {
            e.printStackTrace();
        }
    }

    /**
     * 将一条人感数据和本数据聚合
     */
    public void Reduce(OccCubeModels entity) {
        if (this.Time.before(entity.Time)) {
            this.Time = entity.Time;
        }
        this.Lux = (entity.Lux + this.Lux) / 2.0;
        //false的人感才会被替代
        this.Occupancy = this.Occupancy || entity.Occupancy;
    }

    @Override
    public String toString() {
        return com.alibaba.fastjson.JSONObject.toJSONString(this);
    }

}
