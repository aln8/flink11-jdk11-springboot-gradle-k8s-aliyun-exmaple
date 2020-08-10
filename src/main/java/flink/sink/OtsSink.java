package flink.sink;

import com.alicloud.openservices.tablestore.SyncClient;
import com.alicloud.openservices.tablestore.model.*;
import flink.config.OtsConfig;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Slf4j
public class OtsSink<In> extends RichSinkFunction<In> implements CheckpointedFunction {
    private final static Logger LOG = LoggerFactory.getLogger(OtsSink.class);

    private transient SyncClient otsClient;
    private OtsConfig config;
    private OtsSchema<In> schema;

    public OtsSink(OtsSchema<In> schema, OtsConfig config) {
        this.schema = schema;
        this.config = config;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        this.otsClient = new SyncClient(this.config.getEndPoint(),
                this.config.getAccessKey(),
                this.config.getSecretKey(),
                this.config.getInstanceName());
    }

    @Override
    public void close() throws Exception {
        super.close();
        this.otsClient.shutdown();
    }

    @Override
    public void invoke(In input, SinkFunction.Context context) {
        try {
            RowPutChange changeRequest = this.schema.put(input);
            changeRequest.setTableName(this.config.getTableName());
            this.otsClient.putRow(new PutRowRequest(changeRequest));
        } catch (Exception e) {
            LOG.error("Write to OTS failed!", e);
        }
    }

    @Override
    public void snapshotState(FunctionSnapshotContext context) throws Exception {
        // Nothing to do
    }

    @Override
    public void initializeState(FunctionInitializationContext context) throws Exception {
        // Nothing to do
    }
}
