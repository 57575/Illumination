package Illumination.utils;

import Illumination.models.WarningReceiver;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import kong.unirest.Unirest;

import java.text.SimpleDateFormat;
import java.util.*;

public class PostMessage {

    private static HashMap<String, Long> messageHistory = new HashMap<>();
    private static SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

    private static String Host;
    private static String ProjectId;
    private static String TemplateId;
    private static List<WarningReceiver> WarningReceivers = new ArrayList<>();

    public static void SetWarningParameters(JSONObject parameter) {
        Host = parameter.getString("host");
        ProjectId = parameter.getInteger("projectId").toString();
        TemplateId = parameter.getInteger("templateId").toString();
        if (WarningReceivers == null) {
            WarningReceivers = new ArrayList<>();
        }
        WarningReceivers = parameter.getJSONArray("receivers").toJavaList(WarningReceiver.class);
    }


    public static void PostWarningMessage(String key, String message, long timeStamp) {
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

    private static boolean PostMessage(String message, String timeStr) {
        try {
            String route = "/api/messagecenter/projects/{projectId}/messages/templateId/{templateId}";
            Unirest.post(Host + route)
                    .routeParam("projectId", ProjectId)
                    .routeParam("templateId", TemplateId)
                    .header("Content-Type", "application/json")
                    .header("Authorization", GetToken())
                    .body(GetBody(message, timeStr))
                    .asString();
        } catch (Exception e) {

        }
        return true;
    }

    private static String GetBody(String message, String timeStr) {
        JSONObject body = new JSONObject();
        JSONObject sender = new JSONObject();
        sender.put("type", "系统");
        sender.put("id", 0);
        sender.put("name", "照明异常检测");
        body.put("sender", sender);

        List<JSONObject> receivers = new ArrayList<>();
        for (WarningReceiver p : WarningReceivers
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
