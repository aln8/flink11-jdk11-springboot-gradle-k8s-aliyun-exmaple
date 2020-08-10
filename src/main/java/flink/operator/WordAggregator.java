package flink.operator;

import flink.model.WordSink;
import flink.model.WordSource;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.springframework.stereotype.Component;

@Slf4j
@Component
public class WordAggregator implements AggregateFunction<WordSource, WordSink, WordSink> {

    @Override
    public WordSink createAccumulator() {
        return new WordSink();
    }

    @Override
    public WordSink add(WordSource source, WordSink agg) {
        if (agg.getWord() == null) {
            agg.setWord(source.getWord());
            agg.setCount(0);
        }
        int count = agg.getCount();
        agg.setCount(count + 1);

        return agg;
    }

    @Override
    public WordSink getResult(WordSink acc) {
        return acc;
    }

    @Override
    public WordSink merge(WordSink a, WordSink b) {
        int count = a.getCount() + b.getCount();
        a.setCount(count);
        return a;
    }


}

