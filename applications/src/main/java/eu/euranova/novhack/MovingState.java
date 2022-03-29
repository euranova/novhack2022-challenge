package eu.euranova.novhack;

import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;

public class MovingState extends RichFlatMapFunction<Row, Row> {

    private final String key;
    private final String variable;
    private final String timestamp;


    public MovingState(String key, String variable, String timestamp){
        this.key = key;
        this.variable = variable;
        this.timestamp = timestamp;
    }

    /**
     * The ValueState handle. The first field is the count, the second field a running sum.
     */
    private transient ValueState<Tuple2<Double, Long>> valueState;

    @Override
    public void flatMap(Row new_value, Collector<Row> collector) throws Exception {

        // access the state value
        Tuple2<Double, Long> previousValue = valueState.value();
        double output;

        Object currentVariableTmp = new_value.getFieldAs(this.variable);
        double currentVariable;
        Long currentVariableLong;

        if (currentVariableTmp instanceof Long)
            currentVariable = ((Long) currentVariableTmp).doubleValue();
        else
            currentVariable = new_value.getFieldAs(this.variable);


        Timestamp currentTimestampTmp = new_value.getFieldAs(this.timestamp);
        Long currentTimestamp = currentTimestampTmp.getTime();


        if (previousValue == null){
            output = -1;
        }
        else{
            // timestamp are in milliseconds not seconds
            output = (currentVariable - previousValue.f0) / (currentTimestamp - previousValue.f1) * 1000.0;
        }


        // update the count
        Tuple2<Double, Long> currentValue = Tuple2.of(
                currentVariable,
                currentTimestamp
        );

        // update the state
        valueState.update(currentValue);

        Row out = Row.withNames(new_value.getKind());
        out.setField("trip_id", new_value.getFieldAs(this.key));
        out.setField("timestamp", new_value.getFieldAs(this.timestamp));
        out.setField("res", output);

        collector.collect(out);
//        return out;
    }

    @Override
    public void open(Configuration config) {
        ValueStateDescriptor<Tuple2<Double, Long>> descriptor =
                new ValueStateDescriptor<Tuple2<Double, Long>>(
                        "average", // the state name
                        TypeInformation.of(new TypeHint<Tuple2<Double, Long>>() {
                        }) // type information
                );
        valueState = getRuntimeContext().getState(descriptor);
    }


//    @Override
//    public void flatMap(Row row, Collector<Row> collector) throws Exception {
//
//    }
}