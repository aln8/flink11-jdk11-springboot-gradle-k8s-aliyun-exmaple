package flink.model;

import com.alicloud.openservices.tablestore.model.*;
import flink.sink.OtsSchema;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class WordSinkSerializationSchema implements OtsSchema<WordSink> {

    @Override
    public RowPutChange put(WordSink ws) {
        PrimaryKeyBuilder pkBuilder = PrimaryKeyBuilder.createPrimaryKeyBuilder();
        pkBuilder.addPrimaryKeyColumn("word", PrimaryKeyValue.fromString(ws.getWord()));
        PrimaryKey pk = pkBuilder.build();

        RowPutChange rowPutChange = new RowPutChange("tmp", pk);

        rowPutChange.addColumn("count", new ColumnValue(ws.getCount(), ColumnType.INTEGER));
        log.info("Put record to OTS: {}", ws.getWord());

        return rowPutChange;
    }
}
