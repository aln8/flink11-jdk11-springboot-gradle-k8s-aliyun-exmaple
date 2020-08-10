package flink;

import flink.config.MqConfig;
import flink.config.OtsConfig;
import flink.model.WordSink;
import flink.model.WordSinkSerializationSchema;
import flink.model.WordSource;
import flink.model.WordSourceDeserializationSchema;
import flink.operator.WordAggregator;
import flink.sink.OtsSink;
import flink.source.RocketMQSource;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.stereotype.Component;

@Component
public class AutoConfig {

    @Bean("flinkEnvironment")
    StreamExecutionEnvironment flinkEnvironment() {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(30000);

        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        return env;
    }

    @Bean("otsConfig")
    @ConfigurationProperties("ots")
    OtsConfig otsConfig() {
        return new OtsConfig();
    }

    @Bean("mqConfig")
    @ConfigurationProperties("rocketmq")
    MqConfig mqConfig() {
        return new MqConfig();
    }

    @Bean("sink")
    OtsSink<WordSink> sink(OtsConfig otsConfig) {
        return new OtsSink<>(new WordSinkSerializationSchema(), otsConfig);
    }

    @Bean("source")
    RocketMQSource<WordSource> source(MqConfig mqConfig) {
        return new RocketMQSource<>(new WordSourceDeserializationSchema(), mqConfig);
    }

    @Bean("aggregator")
    AggregateFunction<WordSource, WordSink, WordSink> aggregator() {
        return new WordAggregator();
    }
}