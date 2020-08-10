package flink;

import flink.model.WordSink;
import flink.model.WordSource;
import flink.sink.OtsSink;
import flink.source.RocketMQSource;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.springframework.boot.ApplicationRunner;
import org.springframework.context.annotation.Bean;
import org.springframework.stereotype.Component;

@Component
public class Executor {
    @Bean
    ApplicationRunner applicationRunner(
            StreamExecutionEnvironment flinkEnvironment,
            RocketMQSource source,
            AggregateFunction<WordSource, WordSink, WordSink> aggregator,
            OtsSink<WordSink> sink
    ) {
        return args -> {
            flinkEnvironment
                    .addSource(source)
                    .name("WordCountSource")
                    .keyBy("word")
                    .timeWindow(Time.minutes(1))
                    .aggregate(aggregator)
                    .addSink(sink).name("WordCountSink");

            flinkEnvironment.execute("WordCountExample");
        };
    }
}
