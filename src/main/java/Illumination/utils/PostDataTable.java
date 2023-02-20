package Illumination.utils;

import kong.unirest.Headers;
import kong.unirest.HttpResponse;
import kong.unirest.Unirest;
import org.slf4j.Logger;

public class PostDataTable {

    public static void AddTableData(String host, String projectId, String type, String tableName, String jsonData, Logger log) {
        try {
            String route = "/api/datapipeline/projects/{projectId}/data/{type}/{nameOrId}";
            Headers headers = Unirest.config().getDefaultHeaders();
            if (!headers.containsKey("flinkId")) {
                Unirest.config().addDefaultHeader("flinkId", System.getenv("FLINK_ID"));
                Unirest.config().addDefaultHeader("flinkSecret", System.getenv("FLINK_SECRET"));
            }
            HttpResponse<String> response = Unirest.post(host + route)
                    .routeParam("projectId", projectId)
                    .routeParam("type", type)
                    .routeParam("nameOrId", tableName)
                    .header("Content-Type", "application/json")
                    .body(jsonData)
                    .asString();
            log.info("写入数据:" + jsonData + ";状态:" + response.getStatus());
        } catch (Exception e) {
            log.info("写入数据失败:" + jsonData + ";错误:" + e.getMessage());
        }
    }
}
