package eu.euranova.novhack;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.connector.jdbc.JdbcStatementBuilder;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.formats.avro.AvroRowDeserializationSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.Tumble;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Timestamp;
import java.time.Duration;
import java.util.Date;

import static org.apache.flink.table.api.Expressions.$;
import static org.apache.flink.table.api.Expressions.lit;


// import org.json.simple.JSONObject;
// import org.json.simple.parser.JSONParser;


public class QuestionEight {


    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        /* Get configs */
        ParameterTool params = ParameterTool.fromArgs(args);
        String configFile = params.get("application.properties");

        ObjectMapper objectMapper = new ObjectMapper();

        FlinkConfig config = objectMapper.readValue(new File(configFile), FlinkConfig.class);

        System.out.println(config.getKafka().getBootstrapServers());

        String brokers = (String) config.getKafka().getBootstrapServers();
        String canTopic = (String) config.getInputTopics().getCan();


        long date = new Date().getTime();
        String consumerId = "can" + "_" + String.valueOf(new Date().getTime());

        Path canPath = Path.of(
                "applications/src/main/avro/can.avsc");

        /* KafkaSource : get streaming of data */
        String canAvroSchemaString = Files.readString(canPath);
        AvroRowDeserializationSchema canRowSchema = new AvroRowDeserializationSchema(canAvroSchemaString);

        KafkaSource<Row> source = KafkaSource.<Row>builder()
                .setBootstrapServers(brokers)
                .setTopics(canTopic)
                .setGroupId(consumerId)
                .setProperty("auto.offset.reset", "earliest")
                .setProperty("security.protocol", config.getKafka().getSecurityProtocol())
                .setProperty("sasl.mechanism", config.getKafka()
                        .getSaslMechanism())
                .setProperty("sasl.jaas.config", config.getKafka()
                        .getSaslJaasConfig())
                .setProperty("sasl.client.callback.handler.class",
                        config.getKafka()
                                .getSaslClientCallbackHandlerClass())
                .setValueOnlyDeserializer(canRowSchema)
                .build();

        /* Question 8 */



//        WatermarkStrategy<Row> wmStrategy =
//                WatermarkStrategy
//                        .<Row>forMonotonousTimestamps()
//                        .withTimestampAssigner((row, timestamp) -> {
//                                    Timestamp tmp = row.getFieldAs("source_timestamp");
//                                    return tmp.getTime();
//                                }
//                        );

        DataStream<Row> canStream = env.fromSource(source, WatermarkStrategy.<Row>forBoundedOutOfOrderness(Duration.ofSeconds(10))
                .withTimestampAssigner((row, timestamp) -> {
                            Timestamp tmp = row.getFieldAs(23); // 23 == "source_timestamp"
                            return tmp.getTime();
                        }
                )
                ,"canSource")
                .filter(row -> row.getFieldAs("trip_id").equals("f5637bd4-6d92-417d-8923-2994d079f17a"));


//        DataStream<Row> canStream2 = canStream1.assignTimestampsAndWatermarks(wmStrategy)
//                .filter(row -> row.getFieldAs("trip_id").equals("f5637bd4-6d92-417d-8923-2994d079f17a"))
//                .filter(row -> row.getFieldAs("source_timestamp").toString().compareTo("2020-06-10 06:30:20.000") < 0 )
//                ;


//        DataStream<Row> canStream = env.fromSource(source, WatermarkStrategy.noWatermarks(),"canSource")
//                .filter(row -> row.getFieldAs("trip_id").equals("f5637bd4-6d92-417d-8923-2994d079f17a"));
//
//
////        canStream2.print();
        Table tableStream = tableEnv.fromDataStream(canStream, $("source_timestamp").rowtime(), $("trip_id"), $("abs"));
        tableStream.printSchema();
        tableEnv.createTemporaryView("mytable", tableStream);
//


//        String query =
//                "SELECT"
//                        + " CAST(TUMBLE_START(source_timestamp, INTERVAL '1' SECOND) AS STRING) window_start,"
//                + " COUNT(*) order_num"
//                + " FROM mytable"
//                + " GROUP BY TUMBLE(source_timestamp, INTERVAL '1' SECONDe)";

//        String query_add_watermark = "CREATE TABLE mytable_with_watermark (" +
//                "WATERMARK FOR source_timestamp AS source_timestamp - INTERVAL '1' SECOND" +
//                ") LIKE mytable";

//        System.out.println("query_add_watermark = " + query_add_watermark);

//        tableEnv.executeSql(query_add_watermark).print();

//        tableEnv.executeSql("DESCRIBE mytable").print();

        // https://nightlies.apache.org/flink/flink-docs-master/docs/dev/table/sql/queries/window-tvf/#tumble
//        String query_bug = "select window_start, window_end, SUM(`abs`) " +
//                " from TABLE(TUMBLE(TABLE mytable, DESCRIPTOR(`timestamp`), INTERVAL '1' SECONDS))" +
//                " GROUP BY window_start, window_end";
//
//        Table non_overlapping_abs = tableEnv.sqlQuery(
//                query_bug
//        );



        Table non_overlapping_abs =  tableStream
                .window(Tumble.over(lit(1000).millis())
                        .on($("source_timestamp"))
                        .as("w"))
                .groupBy($("w"),$("trip_id"))
                .select($("trip_id"), $("abs").count().as("res"));

        DataStream<Row> non_overlapping_abs_Stream = tableEnv.toChangelogStream(non_overlapping_abs);
        non_overlapping_abs_Stream.print();


//        putInDB(non_overlapping_abs_Stream, config);

        env.execute();
    }

    private static void putInDB(DataStream<Row> question_result, FlinkConfig config) {
        /* Sink */

        String hostname = config.getJdbc().getHostname();
        String database = config.getJdbc().getDatabase();
        String username = config.getJdbc().getUsername();
        int batchIntervalMs = config.getJdbc().getBatchIntervalMs();
        String password = config.getJdbc().getPassword();

        String URL = new StringBuffer().append("jdbc:postgresql://").append(hostname)
                .append("/").append(database).toString();

        JdbcStatementBuilder<Row> jdbcStatementBuilder = (statement, row) -> {

            String trip_id = row.getFieldAs("trip_id").toString();
            long res = row.getFieldAs("res");

            statement.setString(1, trip_id);
            statement.setLong(2, res);
        };

        SinkFunction<Row> jdbcSink = JdbcSink.sink(
                "insert into active_abs (trip_id, abs_activated_windows) values (?, ?)",
                jdbcStatementBuilder,
                JdbcExecutionOptions.builder()
//                                         .withBatchSize(1000)
                        .withBatchIntervalMs(batchIntervalMs)
//                                         .withMaxRetries(5)
                        .build(),
                new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                        .withUrl(URL)
                        .withDriverName("org.postgresql.Driver")
                        .withUsername(username)
                        .withPassword(password)
                        .build()
        );

        question_result.print();
//        question_result.addSink(jdbcSink);

    }
}