package Illumination.function.OnlySensor;

import Illumination.models.WindowedOcc;
import Illumination.models.orgins.OpeningApertureCubeModels;
import Illumination.models.outputs.StrategyAbnormalRecord;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.util.Collector;

import java.util.HashMap;
import java.util.Map;

public class OpeningApertureMapFunction implements FlatMapFunction<OpeningApertureCubeModels, StrategyAbnormalRecord> {

    private final Map<String, StrategyAbnormalRecord> unfinishedRecords;
    private final Map<String, WindowedOcc> last20minuteOcc;
    private final Map<String, OpeningApertureCubeModels> lastOpeningAperture;

    public OpeningApertureMapFunction(
            Map<String, StrategyAbnormalRecord> unfinishedRecords,
            Map<String, WindowedOcc> last20minuteOcc,
            Map<String, OpeningApertureCubeModels> lastOpeningAperture
    ) {
        this.unfinishedRecords = unfinishedRecords;
        this.last20minuteOcc = last20minuteOcc;
        this.lastOpeningAperture = lastOpeningAperture;
    }

    @Override
    public void flatMap(OpeningApertureCubeModels item, Collector<StrategyAbnormalRecord> collector) throws Exception {
        lastOpeningAperture.put(item.Key, item);
        //有未结束的异常事件;开度小于0.5;尝试结束异常事件
        if (unfinishedRecords.containsKey(item.Key) && item.OpeningAperture <= 0.5) {
            StrategyAbnormalRecord record = unfinishedRecords.get(item.Key);
            record.SetFinish(item.Time.getTime());
            record.SetCarbon(1, "kg");
            collector.collect(record);
            unfinishedRecords.remove(item.Key);
            System.out.println("关闭报警事件");
            System.out.println(record.toString());
        }
    }
}
