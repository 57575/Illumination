package Illumination.models.orgins;

import com.alibaba.fastjson.JSONObject;

import java.io.Serializable;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

public class OpeningApertureCubeModels implements Serializable {
    private static final long serialVersionUID = 8256716618301012433L;
    public Date Time;
    public String Key;
    public int ProjectId;
    public double OpeningAperture;

    public void Import(JSONObject entity) {
        this.Key = entity.getString("switch_4");
        this.ProjectId = entity.getInteger("_projectId");
        Object v = entity.get("value");
        if (v instanceof Number) {
            this.OpeningAperture = ((Number) v).doubleValue();
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
        return JSONObject.toJSONString(this);
    }
}
