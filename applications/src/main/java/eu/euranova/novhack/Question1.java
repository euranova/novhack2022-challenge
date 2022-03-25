package eu.euranova.novhack;

import com.amazonaws.services.kinesisanalytics.runtime.KinesisAnalyticsRuntime;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.io.InputStream;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;



public class Question1 {

    public static DataStream<Row> run_with_table(DataStream<Row> canStream, StreamTableEnvironment tableEnv) {
    /**
     * Convert dashboard_speed in km/h with Table API
     *
     * @param  canStream  Datastream from "can" topic
     * @param  tableEnv Flink TableEnvironment for using Table API
     * @return      DataStream<Row> with (timestamp, trip id, speed in km/h)
     */

        /* First Way of doing it: Using Table API */

        tableEnv.createTemporaryView("mytable", canStream);
        Table mytable = tableEnv.sqlQuery("select `timestamp`, trip_id, CAST(dashboard_speed / 100.0 as DOUBLE) as res from mytable");
        DataStream<Row> question1_fromTable = tableEnv.toChangelogStream(mytable);

        return question1_fromTable;

    }

    public static DataStream<Row> run_with_datastream(DataStream<Row> canStream, StreamExecutionEnvironment env) {
        /**
         * Convert dashboard_speed in km/h with DataStream API
         *
         * @param  canStream  Datastream from "can" topic
         * @param  env Flink Environment
         * @return      DataStream<Row> with (timestamp, trip id, speed in km/h)
         */

        /* Second Way of doing it: Using DataStream */

        DataStream<Row> question1_fromDataStream = canStream.map(new MapFunction<Row, Row>() {
            @Override
            public Row map(Row row) throws Exception {

                Long speed = row.getFieldAs("dashboard_speed");

                double result = speed.doubleValue() / 100.0;

                Row out = Row.withNames(row.getKind());
                out.setField("timestamp", row.getFieldAs("timestamp"));
                out.setField("trip_id", row.getFieldAs("trip_id"));
                out.setField("res", result);
                return out;

            }
        });

        return question1_fromDataStream;
    }

}
