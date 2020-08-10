package flink.source;

import org.apache.flink.api.java.typeutils.ResultTypeQueryable;

import java.io.Serializable;

public interface DeserializationSchema<T> extends ResultTypeQueryable<T>, Serializable {
    T deserialize(byte[] key);
}
