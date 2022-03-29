package eu.euranova.novhack;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.table.functions.AggregateFunction;

import java.sql.Timestamp;

// function that takes (value BIGINT, weight INT), stores intermediate results in a structured
// type of WeightedAvgAccumulator, and returns the weighted average as BIGINT
public class WeightedAvg extends AggregateFunction<Tuple3<Timestamp, Double,Long>, WeightedAvgAccumulator> {

    @Override
    public WeightedAvgAccumulator createAccumulator() {
        return new WeightedAvgAccumulator();
    }

    @Override
    public Tuple3<Timestamp, Double,Long> getValue(WeightedAvgAccumulator acc) {
        if (!acc.flag) {
            return null ;
        } else {
            Timestamp tmpStartBrake = acc.startBrake;
            Long tmpBrakeTime = acc.brakeTime;
            acc.startBrake = null;
            acc.brakeTime = 0;
            acc.flag = false;
            return Tuple3.of(tmpStartBrake, acc.avg, tmpBrakeTime);
        }
    }

    public void accumulate(WeightedAvgAccumulator acc, Long iBrakes, Timestamp iTimestamp) {
        if (iBrakes > 0 && acc.startBrake == null){
            acc.startBrake = iTimestamp;
        }
        if (acc.startBrake != null && iBrakes == 0){
            long brakeTime = iTimestamp.getTime() - acc.startBrake.getTime();
            acc.brakeTime = brakeTime;
            if (brakeTime > 1000) { // en millisecond
                // Update acc.avg & reset startBrake and count
                acc.avg = acc.avg + (brakeTime - acc.avg) / (acc.count+1);
                acc.count += 1;
                acc.flag = true;
            }
        }
    }

    public void retract(WeightedAvgAccumulator acc, Long iBrakes, Timestamp iTimestamp) {
//        acc.avg -= iValue;
//        acc.timestamp = iWeight;
    }

    public void merge(WeightedAvgAccumulator acc, Iterable<WeightedAvgAccumulator> it) {
//        for (WeightedAvgAccumulator a : it) {
//            acc.timestamp = a.timestamp;
//            acc.avg += a.avg;
//        }
    }

    public void resetAccumulator(WeightedAvgAccumulator acc) {
        acc.startBrake = null;
        acc.avg = 0.0;
        acc.count = 0;
    }
}