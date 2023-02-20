package Illumination.function;

import Illumination.models.WindowedIllumination;
import Illumination.models.outputs.StrategyAbnormalRecord;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;

public class AbnormalRecordMapFunction implements FlatMapFunction<WindowedIllumination, StrategyAbnormalRecord> {

    private final Logger log;

    public AbnormalRecordMapFunction(Logger logger) {
        this.log = logger;
    }

    @Override
    public void flatMap(WindowedIllumination windowedIllumination, Collector<StrategyAbnormalRecord> collector) throws Exception {
        System.out.println(windowedIllumination.toString());
        if (windowedIllumination.IsWarning) {
            StrategyAbnormalRecord record = new StrategyAbnormalRecord(
                    windowedIllumination.TimeStamp,
                    "无人开启照明",
                    "照明系统",
                    windowedIllumination.Name,
                    "策略",
                    windowedIllumination.Message,
                    windowedIllumination.OriginalData
            );
            collector.collect(record);
        }
    }
}
