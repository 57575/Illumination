package Illumination.utils;

import Illumination.models.WarningReceiver;
import Illumination.models.WindowedIllumination;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import kong.unirest.HttpResponse;
import kong.unirest.Unirest;
import org.apache.flink.api.common.functions.MapFunction;
import org.slf4j.Logger;

import java.text.SimpleDateFormat;
import java.util.*;

public class PostMessage implements MapFunction<WindowedIllumination, WindowedIllumination> {

    private HashMap<String, Long> messageHistory = new HashMap<>();
    private SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    private final Logger Log;
    private String Host;
    private String ProjectId;
    private String TemplateId;
    private String WarningReceivers;
    private Long StartPost;

    public PostMessage(JSONObject parameter, Logger logger) {
        Log = logger;
        Host = parameter.getString("host");
        ProjectId = parameter.getInteger("projectId").toString();
        TemplateId = parameter.getInteger("templateId").toString();
        WarningReceivers = parameter.getJSONArray("receivers").toJSONString();
        long l = parameter.getLong("startTimeStamp");
        if (l < 100000000000L) l *= 1000;
        StartPost = l;
    }

    @Override
    public WindowedIllumination map(WindowedIllumination item) throws Exception {
        if (item.IsWarning) {
            String message = "照明设备" + item.Name + "存在灯光开启无人值守情况";
            PostWarningMessage(item.Name, message, item.TimeStamp);
        }
        return item;
    }

    private void PostWarningMessage(String key, String message, long timeStamp) {
        if (timeStamp < 100000000000L) timeStamp *= 1000;
        if (StartPost <= timeStamp) {
            Date date = new Date(timeStamp);
            sdf.setTimeZone(TimeZone.getTimeZone("Asia/Shanghai"));
            String timeStr = sdf.format(date);
            if (messageHistory.containsKey(key)) {
                if (messageHistory.get(key) + 24 * 60 * 60 * 1000 < timeStamp) {
                    PostMessage(message, timeStr);
                    messageHistory.put(key, timeStamp);
                }
            } else {
                PostMessage(message, timeStr);
                messageHistory.put(key, timeStamp);
            }
        }
    }

    private boolean PostMessage(String message, String timeStr) {
        try {
            String route = "/api/messagecenter/projects/{projectId}/messages/templateId/{templateId}";
            HttpResponse<String> response = Unirest.post(Host + route)
                    .routeParam("projectId", ProjectId)
                    .routeParam("templateId", TemplateId)
                    .header("Content-Type", "application/json")
                    .header("Authorization", GetToken())
                    .body(GetBody(message, timeStr))
                    .asString();
            Log.info("发出报警:" + message + ";时间:" + timeStr + ";结果:" + response.getStatus());
        } catch (Exception e) {
            Log.error("发出报警失败:" + e.getMessage());
        }
        return true;
    }

    private String GetBody(String message, String timeStr) {
        JSONObject body = new JSONObject();
        JSONObject sender = new JSONObject();
        sender.put("type", "系统");
        sender.put("id", 0);
        sender.put("name", "照明异常检测");
        body.put("sender", sender);

        List<JSONObject> receivers = new ArrayList<>();
        List<WarningReceiver> warningReceiverList = JSONArray.parseArray(WarningReceivers).toJavaList(WarningReceiver.class);
        for (WarningReceiver p : warningReceiverList
        ) {
            JSONObject receiver = new JSONObject();
            receiver.put("type", p.type);
            receiver.put("id", p.id);
            receiver.put("name", p.name);
            receivers.add(receiver);
        }

        body.put("receivers", receivers);

        body.put("priority", "高");

        JSONObject parameters = new JSONObject();
        parameters.put("name", "照明异常");
        parameters.put("message", message);
        parameters.put("datetime", timeStr);
        body.put("parameters", parameters);
        return JSON.toJSONString(body);
    }


    private static String GetToken() {
        return "Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpZCI6IjEiLCJwaWQiOiIwIiwibmFtZSI6Iui2hee6p-aXoOaVjOeuoeeQhuWRmCIsImNpZCI6IjEiLCJyb290IjoiMSIsIm5iZiI6MTY2ODY3NTI1MiwiZXhwIjoxOTg0MDM1MjUyLCJpYXQiOjE2Njg2NzUyNTIsImlzcyI6IlNDQzQifQ.-4odE4nOqPWEPwZG5QTBz1iOIYqPt4OCTYZySbE0vW4";
    }


}
