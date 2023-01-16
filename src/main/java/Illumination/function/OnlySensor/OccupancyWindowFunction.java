package Illumination.function.OnlySensor;

import Illumination.models.WindowedOcc;
import Illumination.models.orgins.OccCubeModels;
import Illumination.models.orgins.OpeningApertureCubeModels;
import Illumination.models.outputs.StrategyAbnormalRecord;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.Map;

public class OccupancyWindowFunction extends ProcessWindowFunction<OccCubeModels, Object, String, TimeWindow> {
    private final Map<String, StrategyAbnormalRecord> unfinishedRecords;
    private final Map<String, WindowedOcc> last20minuteOcc;
    private final Map<String, OpeningApertureCubeModels> lastOpeningAperture;
    private final String OperatorName;

    public OccupancyWindowFunction(
            Map<String, StrategyAbnormalRecord> unfinishedRecords,
            Map<String, WindowedOcc> last20minuteOcc,
            Map<String, OpeningApertureCubeModels> lastOpeningAperture,
            String OperatorName) {
        this.unfinishedRecords = unfinishedRecords;
        this.last20minuteOcc = last20minuteOcc;
        this.lastOpeningAperture = lastOpeningAperture;
        this.OperatorName = OperatorName;
    }

    @Override
    public void process(String s, Context context, Iterable<OccCubeModels> rengans, Collector<Object> collector) throws Exception {
        System.out.println("开始聚合窗口,时间:" + System.currentTimeMillis());
        //寻找key
        String key = rengans.iterator().next().Key;
        //聚合本时间窗的人感数据，并添加至聚合Map
        OccCubeModels reducedRengan = rengans.iterator().next();
        for (OccCubeModels rengan : rengans) {
            reducedRengan.Reduce(rengan);
            System.out.println(rengan.toString());
        }
        WindowedOcc windowedOcc = new WindowedOcc();
        if (last20minuteOcc.containsKey(key)) {
            windowedOcc = last20minuteOcc.get(key);
        }
        windowedOcc.AddNewItem(reducedRengan);
        last20minuteOcc.put(key, windowedOcc);
        System.out.println(windowedOcc.toString());

        if (lastOpeningAperture.containsKey(key)) {
            OpeningApertureCubeModels lastOpen = lastOpeningAperture.get(key);
            //上一个开度为开；人感为假；报警
            if (lastOpen.OpeningAperture > 0.5 && (!windowedOcc.Occupancy)) {
                JSONObject originalData = new JSONObject();
                originalData.put("OpeningAperture", lastOpen);
                originalData.put("Occupancy", windowedOcc);
                StrategyAbnormalRecord record = new StrategyAbnormalRecord(
                        lastOpen.Time.getTime(),
                        "无人值守",
                        "照明系统",
                        lastOpen.Key,
                        OperatorName,
                        "",
                        originalData);
                unfinishedRecords.put(key, record);
//                                PostWarning.PostWarningMessage(parameters.Warning, record, logger);
                System.out.println("产生报警事件");
                System.out.println(record.toString());
            }
        }


        System.out.println("结束聚合窗口,时间:" + System.currentTimeMillis());
    }
}
