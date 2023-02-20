package Illumination.models;

import Illumination.models.orgins.OccCubeModels;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;
import java.util.function.Predicate;

public class WindowedOcc {
    public boolean Occupancy;
    public double Lux;
    public String Key;
    public List<OccCubeModels> items;
    /**
     * 该聚合人感数据中，所代表的数据的持续时间，以毫秒计
     */
    public long Duration;

    public WindowedOcc() {
        this.items = new ArrayList<>();
    }

    public void AddNewItem(OccCubeModels data) {
        items.removeIf(new Predicate<OccCubeModels>() {
            @Override
            public boolean test(OccCubeModels item) {
                return item.Time.getTime() + 20 * 60 * 1000 <= data.Time.getTime();
            }
        });
        items.add(data);

        boolean occ = false;
        double lux = 0;
        for (OccCubeModels item : items) {
            occ = occ || item.Occupancy;
            lux = lux + item.Lux;
        }
        this.Occupancy = occ;
        this.Lux = lux / items.size();
        this.Key = data.Key;
    }

    @Override
    public String toString() {
        return com.alibaba.fastjson.JSONObject.toJSONString(this);
    }

}
