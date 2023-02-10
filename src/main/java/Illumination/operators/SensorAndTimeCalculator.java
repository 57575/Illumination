package Illumination.operators;

import Illumination.models.orgins.OccCubeModels;
import Illumination.models.orgins.OpeningApertureCubeModels;
import Illumination.models.outputs.StrategyAbnormalRecord;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.streaming.api.functions.co.CoFlatMapFunction;
import org.apache.flink.util.Collector;

import java.util.*;

public class SensorAndTimeCalculator implements CoFlatMapFunction<OccCubeModels, OpeningApertureCubeModels, StrategyAbnormalRecord> {
    private static final long serialVersionUID = -1203496215980446751L;
    //匹配队列
    private final Map<String, Queue<OccCubeModels>> occMap;
    private final Map<String, OpeningApertureCubeModels> lastOpeningAperture;
    private final Map<String, StrategyAbnormalRecord> unfinishedRecords;
    //
    private final long programStart;
    private final String operator;
    private final Map<String, Double> sensorPower;
    private final double carbonEmissionFactor;
    //排程
    private final int hourStart;
    private final int hourEnd;
    private final int minuteStart;
    private final int minuteEnd;


    public SensorAndTimeCalculator(
            String operatorName,
            Map<String, Double> sensorPower,
            double carbonEmissionFactor,
            int hourStart,
            int hourEnd,
            int minuteStart,
            int minuteEnd
    ) {
        occMap = new HashMap<>();
        lastOpeningAperture = new HashMap<>();
        unfinishedRecords = new HashMap<>();
        this.programStart = System.currentTimeMillis();
        this.operator = operatorName;
        this.sensorPower = sensorPower;
        this.carbonEmissionFactor = carbonEmissionFactor;
        this.hourStart = hourStart;
        this.hourEnd = hourEnd;
        this.minuteStart = minuteStart;
        this.minuteEnd = minuteEnd;
    }

    @Override
    public void flatMap1(OccCubeModels item, Collector<StrategyAbnormalRecord> collector) throws Exception {
        Queue<OccCubeModels> occQueue = new LinkedList<>();
        if (occMap.containsKey(item.Key)) {
            occQueue = occMap.get(item.Key);
        }
        OpeningApertureCubeModels lastOpenObject = new OpeningApertureCubeModels();
        boolean lastOpenValue = false;
        if (lastOpeningAperture.containsKey(item.Key)) {
            lastOpenObject = lastOpeningAperture.get(item.Key);
            //开度大于5才认为是开启
            lastOpenValue = lastOpenObject.OpeningAperture >= 5;
        }

        //过时数据退队
        while ((!occQueue.isEmpty()) && (occQueue.peek().Time.getTime() + 20 * 60 * 1000 < item.Time.getTime())) {
            occQueue.remove();
        }

        boolean waitClose = unfinishedRecords.containsKey(item.Key);
        //当前人感为真,人感入队，并尝试结束未结束事件
        if (item.Occupancy) {
            occQueue.add(item);
            occMap.put(item.Key, occQueue);
            //人感为真，且存在未结束的事件时，结束报警
            if (waitClose) {
                StrategyAbnormalRecord record = unfinishedRecords.get(item.Key);
                record.SetFinish(item.Time.getTime());
                record.SetCarbon(CalculateCarbonEmission(item.Key, CalculateNextStartGap(item.Time.getTime())), "kg");
                collector.collect(record);
                unfinishedRecords.remove(item.Key);
            }
        }
        //当前人感为假
        else {
            //上个开度为开 && 不在排程内，即非工作时间 && 过去二十分钟没有人感为真 && 没有等待结束的事件 && 上一个开度达到时间在该人感数据到达时间的60秒前 && 程序已运行超20分钟
            if (lastOpenValue
                    && (!InSchedule(item.Time.getTime()))
                    && occQueue.isEmpty()
                    && (!waitClose)
                    && (lastOpenObject.Time.getTime() + 60 * 1000 < item.Time.getTime())
                    && (programStart + 20 * 60 * 1000 < item.Time.getTime())
            ) {
                JSONObject originalData = new JSONObject();
                originalData.put("OpeningAperture", lastOpenObject);
                originalData.put("Occupancy", item);
                StrategyAbnormalRecord record = new StrategyAbnormalRecord(
                        item.Time.getTime(),
                        "无人值守",
                        "照明系统",
                        item.Key,
                        operator,
                        "programStartTime:" + programStart,
                        originalData);
                unfinishedRecords.put(item.Key, record);
                collector.collect(record);
            }
        }
    }

    @Override
    public void flatMap2(OpeningApertureCubeModels item, Collector<StrategyAbnormalRecord> collector) throws Exception {
        lastOpeningAperture.put(item.Key, item);
        //有未结束的异常事件;开度小于5;尝试结束异常事件
        if (unfinishedRecords.containsKey(item.Key) && item.OpeningAperture < 5) {
            StrategyAbnormalRecord record = unfinishedRecords.get(item.Key);
            record.SetFinish(item.Time.getTime());
            record.SetCarbon(CalculateCarbonEmission(item.Key, CalculateNextStartGap(item.Time.getTime())), "kg");
            collector.collect(record);
            unfinishedRecords.remove(item.Key);
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
