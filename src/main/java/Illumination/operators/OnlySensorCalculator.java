package Illumination.operators;

import Illumination.models.orgins.OccCubeModels;
import Illumination.models.orgins.OpeningApertureCubeModels;
import Illumination.models.outputs.StrategyAbnormalRecord;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.functions.co.CoFlatMapFunction;
import org.apache.flink.streaming.api.functions.co.RichCoFlatMapFunction;
import org.apache.flink.util.Collector;
import redis.clients.jedis.Jedis;

import java.util.*;

public class OnlySensorCalculator extends RichCoFlatMapFunction<OccCubeModels, OpeningApertureCubeModels, StrategyAbnormalRecord> {

    private static final long serialVersionUID = 5029759832903418584L;
    private MapState<String, Queue<OccCubeModels>> occMap;
    private MapState<String, OpeningApertureCubeModels> lastOpeningAperture;
    private MapState<String, StrategyAbnormalRecord> unfinishedRecords;
    private final long programStart;
    private final String operator;
    private final Map<String, Double> sensorPower;
    private final double carbonEmissionFactor;

    private String taskId;

    public OnlySensorCalculator(String taskId, String operatorName, Map<String, Double> sensorPower, double carbonEmissionFactor) {
        programStart = System.currentTimeMillis();
        this.taskId = taskId;
        this.operator = operatorName;
        this.sensorPower = sensorPower;
        this.carbonEmissionFactor = carbonEmissionFactor;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        MapStateDescriptor<String, Queue<OccCubeModels>> occMapState
                = new MapStateDescriptor<String, Queue<OccCubeModels>>(
                taskId + "occMap",
                TypeInformation.of(new TypeHint<String>() {
                }),
                TypeInformation.of(new TypeHint<Queue<OccCubeModels>>() {
                })
        );
        occMap = getRuntimeContext().getMapState(occMapState);

        MapStateDescriptor<String, OpeningApertureCubeModels> lastOpeningApertureMap
                = new MapStateDescriptor<String, OpeningApertureCubeModels>(
                taskId + "lastOAMap",
                TypeInformation.of(new TypeHint<String>() {
                }),
                TypeInformation.of(new TypeHint<OpeningApertureCubeModels>() {
                })
        );
        lastOpeningAperture = getRuntimeContext().getMapState(lastOpeningApertureMap);

        MapStateDescriptor<String, StrategyAbnormalRecord> unfinishedRecordMap
                = new MapStateDescriptor<String, StrategyAbnormalRecord>(
                taskId + "lastUnfinishedRecordMap",
                TypeInformation.of(new TypeHint<String>() {
                }),
                TypeInformation.of(new TypeHint<StrategyAbnormalRecord>() {
                })
        );
        unfinishedRecords = getRuntimeContext().getMapState(unfinishedRecordMap);
    }

    @Override
    public void flatMap1(OccCubeModels item, Collector<StrategyAbnormalRecord> collector) throws Exception {
        Queue<OccCubeModels> occQueue = new LinkedList<>();
        if (occMap.contains(item.Key)) {
            occQueue = occMap.get(item.Key);
        }
        OpeningApertureCubeModels lastOpenObject = new OpeningApertureCubeModels();
        boolean lastOpen = false;
        if (lastOpeningAperture.contains(item.Key)) {
            lastOpenObject = lastOpeningAperture.get(item.Key);
            //开度大于5才认为是开启，小于5时认为已关闭
            lastOpen = lastOpenObject.OpeningAperture >= 5;
        }

        //过时数据退队
        while ((!occQueue.isEmpty()) && (occQueue.peek().Time.getTime() + 20 * 60 * 1000 < item.Time.getTime())) {
            occQueue.remove();
        }
        //当前人感为真
        if (item.Occupancy) {
            occQueue.add(item);
            occMap.put(item.Key, occQueue);
            //人感为真，且存在未结束的事件时，结束报警
            boolean waitClose = unfinishedRecords.contains(item.Key);
            if (waitClose) {
                StrategyAbnormalRecord record = unfinishedRecords.get(item.Key);
                record.SetFinish(item.Time.getTime());
                record.SetCarbon(CalculateCarbonEmission(item.Key, 5 * 60 * 60), "kg");
                collector.collect(record);
                unfinishedRecords.remove(item.Key);
            }
        }
        //当前人感为假，需要判断
        else {
            //人感数据滞后于开度数据到达，因此，人感到达事件大于开度到达时间60s以上时才任务可能异常
            if (lastOpen && lastOpenObject.Time.getTime() + 60 * 1000 < item.Time.getTime()) {
                boolean waitClose = unfinishedRecords.contains(item.Key);
                //上一个开度值为真;没有需要等待关闭的异常事件;过去二十分钟没有人感为真的数据;发出报警
                //if (lastOpen && waitClose && occQueue.isEmpty() && (programStart + 20 * 60 * 1000 < item.Time.getTime()))
                if ((!waitClose) && occQueue.isEmpty() && (programStart + 20 * 60 * 1000 < item.Time.getTime())) {
                    JSONObject originalData = new JSONObject();
                    originalData.put("OpeningAperture", lastOpenObject);
                    originalData.put("Occupancy", JSONObject.toJSONString(occQueue));
                    StrategyAbnormalRecord record = new StrategyAbnormalRecord(
                            lastOpenObject.Time.getTime(),
                            "无人状态下开启照明",
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
    }

    @Override
    public void flatMap2(OpeningApertureCubeModels item, Collector<StrategyAbnormalRecord> collector) throws Exception {
        lastOpeningAperture.put(item.Key, item);
        //有未结束的异常事件;开度小于5;尝试结束异常事件
        if (unfinishedRecords.contains(item.Key) && item.OpeningAperture < 5) {
            StrategyAbnormalRecord record = unfinishedRecords.get(item.Key);
            record.SetFinish(item.Time.getTime());
            record.SetCarbon(CalculateCarbonEmission(item.Key, 5 * 60 * 60), "kg");
            collector.collect(record);
            unfinishedRecords.remove(item.Key);
        }
    }

    private double CalculateCarbonEmission(String key, long duration) {
        double power = sensorPower.get(key);
        double result = (duration / (60.0 * 60.0)) * (power / 1000.0) * carbonEmissionFactor;
        return result;
    }


}
