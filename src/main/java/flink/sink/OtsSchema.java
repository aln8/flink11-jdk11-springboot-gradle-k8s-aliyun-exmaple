package flink.sink;

import com.alicloud.openservices.tablestore.model.RowPutChange;

import java.io.Serializable;

public interface OtsSchema<IN> extends Serializable {
    RowPutChange put(IN in);
}
