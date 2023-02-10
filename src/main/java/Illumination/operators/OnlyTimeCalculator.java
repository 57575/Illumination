package Illumination.operators;

import Illumination.models.orgins.OccCubeModels;
import Illumination.models.orgins.OpeningApertureCubeModels;
import Illumination.models.outputs.StrategyAbnormalRecord;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.util.Collector;

import java.util.*;

public class OnlyTimeCalculator implements FlatMapFunction<OpeningApertureCubeModels, StrategyAbnormalRecord> {

    private static final long serialVersionUID = 1213564612667547735L;
    //匹配队列
    private final Map<String, StrategyAbnormalRecord> unfinishedRecords;
    //
    private final String operator;
    private final Map<String, Double> sensorPower;
    private final double carbonEmissionFactor;
    //排程
    private final int hourStart;
    private final int hourEnd;
    private final int minuteStart;
    private final int minuteEnd;

    public OnlyTimeCalculator(
            String operatorName,
            Map<String, Double> sensorPower,
            double carbonEmissionFactor,
            int hourStart,
            int hourEnd,
            int minuteStart,
            int minuteEnd
    ) {
        unfinishedRecords = new HashMap<>();
        this.operator = operatorName;
        this.sensorPower = sensorPower;
        this.carbonEmissionFactor = carbonEmissionFactor;
        this.hourStart = hourStart;
        this.hourEnd = hourEnd;
        this.minuteStart = minuteStart;
        this.minuteEnd = minuteEnd;
    }

    @Override
    public void flatMap(OpeningApertureCubeModels item, Collector<StrategyAbnormalRecord> collector) throws Exception {
        boolean waitClose = unfinishedRecords.containsKey(item.Key);
        //开状态进行判断,开度大于5才认为是开启
        if (item.OpeningAperture >= 5) {
            //在排程内，即非工作时间 && 没有等待结束的事件
            if ((!InSchedule(item.Time.getTime())) && (!waitClose)) {
                JSONObject originalData = new JSONObject();
                originalData.put("OpeningAperture", item);
                StrategyAbnormalRecord record = new StrategyAbnormalRecord(
                        item.Time.getTime(),
                        "不在排程内",
                        "照明系统",
                        item.Key,
                        operator,
                        "",
                        originalData);
                unfinishedRecords.put(item.Key, record);
                collector.collect(record);
            }
        } else {
            //有待结束的事件
            if (waitClose) {
                StrategyAbnormalRecord record = unfinishedRecords.get(item.Key);
                record.SetFinish(item.Time.getTime());
                record.SetCarbon(CalculateCarbonEmission(item.Key, CalculateNextStartGap(item.Time.getTime())), "kg");
                collector.collect(record);
                unfinishedRecords.remove(item.Key);
            }
        }
    }


    /**
     * 计算碳排放量
     *
     * @param duration 持续时间，以秒计
     * @return 碳排放，以kg计
     */
    private double CalculateCarbonEmission(String key, long duration) {
        double power = sensorPower.get(key);
        double result = (duration / (60.0 * 60.0)) * (power / 1000.0) * carbonEmissionFactor;
        return result;
    }

    /**
     * 计算从输入时间至下一个日程开始时间的时间间隔
     *
     * @param currentTime 输入时间，以毫秒计
     * @return 以秒计时间
     */
    private long CalculateNextStartGap(long currentTime) {
        Calendar now = Calendar.getInstance(TimeZone.getTimeZone("Asia/Shanghai"));
        now.setTimeInMillis(currentTime);
        Calendar currentDateStart = Calendar.getInstance(TimeZone.getTimeZone("Asia/Shanghai"));
        currentDateStart.set(now.get(Calendar.YEAR), now.get(Calendar.MONTH), now.get(Calendar.DAY_OF_MONTH), hourStart, minuteStart, 0);
        if (now.before(currentDateStart)) {
            return (currentDateStart.getTimeInMillis() - now.getTimeInMillis()) / 1000;
        } else {
            return (currentDateStart.getTimeInMillis() + 24 * 60 * 60 * 1000 - now.getTimeInMillis()) / 1000;
        }
    }

    /**
     * 计算给定时间是否在排程内
     *
     * @return false, 不在工作时间内，需要检测报警;true,在工作时间内，无需检测报警
     */
    private boolean InSchedule(long time) {
        Calendar now = Calendar.getInstance(TimeZone.getTimeZone("Asia/Shanghai"));
        now.setTimeInMillis(time);
        boolean isHoliday = now.get(Calendar.DAY_OF_WEEK) == Calendar.SATURDAY || now.get(Calendar.DAY_OF_WEEK) == Calendar.SUNDAY;
        if (isHoliday) {
            return false;
        }
        int year = now.get(Calendar.YEAR);
        int month = now.get(Calendar.MONTH);
        int day = now.get(Calendar.DAY_OF_MONTH);
        Calendar sameDayStart = Calendar.getInstance(TimeZone.getTimeZone("Asia/Shanghai"));
        sameDayStart.set(year, month, day, hourStart, minuteStart, 0);
        Calendar sameDayEnd = Calendar.getInstance(TimeZone.getTimeZone("Asia/Shanghai"));
        sameDayEnd.set(year, month, day, hourEnd, minuteEnd, 0);
        if (now.before(sameDayStart) || now.after(sameDayEnd)) {
            return false;
        }
        return true;
    }
}
