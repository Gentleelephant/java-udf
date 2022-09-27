package org.example;

import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.FunctionHint;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.types.Row;

import java.util.ArrayList;

import java.util.Map;

public class ParseJson extends ScalarFunction {

    @FunctionHint(input = @DataTypeHint("ROW<telemetry MAP<STRING,ROW<ts BIGINT,value DOUBLE>>>"),output = @DataTypeHint("ARRAY<ROW<eid STRING, ts BIGINT,value DOUBLE>>"))
    public Row[] eval(Row row) {
        Map<String, Row> telemetry = (Map<String, Row>) row.getField(0);
        ArrayList<Row> result = new ArrayList<>();
        telemetry.forEach((k, v) -> {
            Row r = Row.of(k, v.getField(0), v.getField(1));
            result.add(r);
        });
        Row[] rows = new Row[result.size()];
        return result.toArray(rows);
    }

}