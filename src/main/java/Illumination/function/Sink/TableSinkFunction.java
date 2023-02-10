package Illumination.function.Sink;

import Illumination.models.outputs.StrategyAbnormalRecord;
import Illumination.utils.PostDataTable;
import Illumination.utils.PostWarning;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.slf4j.Logger;

public class TableSinkFunction extends RichSinkFunction<StrategyAbnormalRecord> {


    private static final long serialVersionUID = 7189887362248109131L;
    private final Logger log;
    private final String tableHost;
    private final String tableName;
    private final String projectId;
    private final String warningHost;
    private final String receiverJsonStr;
    private final String templateId;

    public TableSinkFunction(String tableHost, String table, String projectId, String warningHost, String receiverJsonStr, String templateId, Logger log) {
        this.log = log;
        this.tableHost = tableHost;
        this.tableName = table;
        this.projectId = projectId;
        this.receiverJsonStr = receiverJsonStr;
        this.templateId = templateId;
        this.warningHost = warningHost;
    }

    @Override
    public void invoke(StrategyAbnormalRecord item, Context context) throws Exception {
        if (item.IsFinish) {
            PostDataTable.AddTableData(tableHost, projectId, "cube", tableName, item.toCubeJsonStr(), log);
        } else {
            PostWarning.PostWarningMessage(warningHost, projectId, templateId, receiverJsonStr, item, log);
            System.out.println("发送报警成功");
        }
    }

}
