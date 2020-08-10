package flink.model;

import flink.model.WordSource;
import flink.source.DeserializationSchema;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.typeinfo.TypeInformation;

@Slf4j
public class WordSourceDeserializationSchema implements DeserializationSchema<WordSource> {
    @Override
    public WordSource deserialize(byte[] value) {
        WordSource source = new WordSource();
        source.setWord(new String(value));
        return null;
    }

    @Override
    public TypeInformation<WordSource> getProducedType() {
        return TypeInformation.of(WordSource.class);
    }
}
