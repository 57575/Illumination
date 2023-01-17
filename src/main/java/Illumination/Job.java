package Illumination;

import Illumination.models.ExternalTask;
import Illumination.operators.OnlySensorOperator;
import Illumination.operators.OnlyTimeOperator;
import Illumination.operators.SensorAndTimeCalculator;
import Illumination.operators.SensorAndTimeOperator;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import kong.unirest.HttpResponse;
import kong.unirest.Unirest;

import java.util.*;


public class Job {

    public static void main(String[] args) throws Exception {

        ExternalTask parameters = GetParameters(args);
        GetSensorsPower(parameters);
        switch (parameters.Operator) {
            case 仅排程:
                OnlyTimeOperator.Run(parameters);
                break;
            case 仅传感器:
                OnlySensorOperator.Run(parameters);
                break;
            case 传感器和排程:
                SensorAndTimeOperator.Run(parameters);
                break;
            default:
                break;
        }
    }

    private static void GetSensorsPower(ExternalTask parameters) throws Exception {
        String route = "/api/datapipeline/projects/" + parameters.ProjectId + "/data/dim/" + parameters.CarbonEmission.getString("DimensionId");
        String url = System.getenv("WEB_URL") + route;
        HttpResponse<String> result = Unirest.get(url).asString();
        if (!result.isSuccess()) throw new Exception("无法查询照明传感器详情,status:" + result.getStatus());
        Map<String, Double> sensorPower = new HashMap<>();
        List<JSONObject> allSensors = JSONArray.parseArray(result.getBody()).toJavaList(JSONObject.class);
        List<String> effectiveSensors = parameters.OpeningApertureList.toJavaList(String.class);
        for (JSONObject sensor : allSensors) {
            String key = sensor.getString("_key");
            if (effectiveSensors.contains(key)) {
                double p = 0;
                Double power = sensor.getDouble("power");
                if (power instanceof Number) {
                    p = power.doubleValue();
                }
                sensorPower.put(key, p);
            }
        }
        parameters.SetSensorPower(sensorPower);

        if (effectiveSensors.size() != sensorPower.size()) {
            throw new Exception("查询得到的照明传感器与该策略配置的不一致");
        }

    }

    private static ExternalTask GetParameters(String[] args) throws Exception {
        if (args.length < 1) throw new Exception("输入参数数量不足");
        String taskId = args[0];
        Unirest.config().addDefaultHeader("flinkId", System.getenv("FLINK_ID"));
        Unirest.config().addDefaultHeader("flinkSecret", System.getenv("FLINK_SECRET"));
        String url = System.getenv("WEB_URL") + "/api/datapipeline/projects/0/externaltask/" + taskId;
        HttpResponse<String> result = Unirest.get(url).asString();
        if (!result.isSuccess()) throw new Exception("无法查询任务详情,status:" + result.getStatus());
        JSONObject task = JSONObject.parseObject(result.getBody());
        if (task == null) throw new Exception("无法查询任务详情,body:" + result.getBody());
        ExternalTask parameters = task.getJSONObject("parametersJson").toJavaObject(ExternalTask.class);
        parameters.TaskId = task.getString("id");
        parameters.FlinkId = System.getenv("FLINK_ID");
        parameters.FlinkSecret = System.getenv("FLINK_SECRET");
        return parameters;
    }


}
