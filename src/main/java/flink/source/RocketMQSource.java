package flink.source;

import com.aliyun.openservices.ons.api.Message;
import com.aliyun.openservices.ons.api.ONSFactory;
import com.aliyun.openservices.ons.api.order.ConsumeOrderContext;
import com.aliyun.openservices.ons.api.order.MessageOrderListener;
import com.aliyun.openservices.ons.api.order.OrderAction;
import com.aliyun.openservices.ons.api.order.OrderConsumer;
import flink.config.MqConfig;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.Validate;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.CheckpointListener;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.streaming.api.operators.StreamingRuntimeContext;
import org.apache.flink.streaming.api.watermark.Watermark;

import java.util.Properties;

/**
 * The RocketMQSource is based on RocketMQ pull consumer mode, and provides exactly once reliability guarantees when
 * checkpoints are enabled. Otherwise, the source doesn't provide any reliability guarantees.
 */
@Slf4j
public class RocketMQSource<OUT> extends RichParallelSourceFunction<OUT>
        implements CheckpointedFunction, CheckpointListener, ResultTypeQueryable<OUT> {

    private static final long serialVersionUID = 1L;
    private OrderConsumer consumer;
    private DeserializationSchema<OUT> schema;
    private RunningChecker runningChecker;
    private transient boolean enableCheckpoint;
    private MqConfig config;

    public RocketMQSource(DeserializationSchema<OUT> schema, MqConfig config) {
        this.schema = schema;
        this.config = config;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        log.debug("source open....");
        Validate.notNull(config, "Consumer properties can not be empty");
        Validate.notNull(schema, "KeyValueDeserializationSchema can not be null");

        Validate.notEmpty(config.getTopic(), "Consumer topic can not be empty");
        Validate.notEmpty(config.getGroup(), "Consumer group can not be empty");

        this.enableCheckpoint = ((StreamingRuntimeContext) getRuntimeContext()).isCheckpointingEnabled();

        runningChecker = new RunningChecker();

        Properties properties = config.RocketMqPropertie();
        //Wait for lite pull consumer
        consumer = ONSFactory.createOrderedConsumer(properties);
    }

    @Override
    public void run(SourceContext<OUT> context) throws Exception {
        log.debug("source run....");
        // The lock that guarantees that record emission and state updates are atomic,
        // from the view of taking a checkpoint.
        final Object lock = context.getCheckpointLock();

        consumer.subscribe(config.getTopic(), config.getTag(),  new MessageOrderListener() {
            @Override
            public OrderAction consume(final Message message, final ConsumeOrderContext ctx) {
                log.debug(message.toString());
                byte[] value = message.getBody();
                OUT data = schema.deserialize(value);
                long time = message.getBornTimestamp();
                synchronized (lock) {
                    context.collectWithTimestamp(data, time);
                    context.emitWatermark(new Watermark(time));
                }
                return OrderAction.Success;
            }
        });

        consumer.start();
        runningChecker.setRunning(true);
        awaitTermination();
    }

    private void awaitTermination() throws InterruptedException {
        while (runningChecker.isRunning()) {
            Thread.sleep(50);
        }
    }

    @Override
    public void cancel() {
        log.debug("cancel ...");
        runningChecker.setRunning(false);

        if (consumer != null) {
            consumer.shutdown();
        }
    }

    @Override
    public void close() throws Exception {
        log.debug("close ...");
        // pretty much the same logic as cancelling
        try {
            cancel();
        } finally {
            super.close();
        }
    }

    @Override
    public void snapshotState(FunctionSnapshotContext context) throws Exception {
        // called when a snapshot for a checkpoint is requested

        if (!runningChecker.isRunning()) {
            log.debug("snapshotState() called on closed source; returning null.");
            return;
        }

        if (log.isDebugEnabled()) {
            log.debug("Snapshotting state {} ...", context.getCheckpointId());
        }

        if (log.isDebugEnabled()) {
            log.debug("Snapshotted state, checkpoint id: {}, timestamp: {}",
                    context.getCheckpointId(), context.getCheckpointTimestamp());
        }
    }

    @Override
    public void initializeState(FunctionInitializationContext context) throws Exception {
        // called every time the user-defined function is initialized,
        // be that when the function is first initialized or be that
        // when the function is actually recovering from an earlier checkpoint.
        // Given this, initializeState() is not only the place where different types of state are initialized,
        // but also where state recovery logic is included.
        log.debug("initialize State ...");

        log.info("No restore state for the consumer.");
    }

    @Override
    public TypeInformation<OUT> getProducedType() {
        return schema.getProducedType();
    }

    @Override
    public void notifyCheckpointComplete(long checkpointId) throws Exception {
        // callback when checkpoint complete
        if (!runningChecker.isRunning()) {
            log.debug("notifyCheckpointComplete() called on closed source; returning null.");
        }
    }
}
