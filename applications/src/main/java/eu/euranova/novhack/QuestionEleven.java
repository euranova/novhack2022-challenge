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
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Date;

import static org.apache.flink.table.api.Expressions.*;


// import org.json.simple.JSONObject;
// import org.json.simple.parser.JSONParser;


public class QuestionEleven {


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
        String fcdsTopic = (String) config.getInputTopics().getFcds();


        long date = new Date().getTime();
        String consumerId = "can" + "_" + String.valueOf(new Date().getTime());


        /* KafkaSource : get streaming of data */
        Path canPath = Path.of(
                "applications/src/main/avro/can.avsc");
        String canAvroSchemaString = Files.readString(canPath);
        AvroRowDeserializationSchema canRowSchema = new AvroRowDeserializationSchema(canAvroSchemaString);

        KafkaSource<Row> canSource = KafkaSource.<Row>builder()
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


        Path fcdsPath = Path.of(
                "applications/src/main/avro/fcds.avsc");
        String fcdsAvroSchemaString = Files.readString(fcdsPath);
        AvroRowDeserializationSchema fcdsRowSchema = new AvroRowDeserializationSchema(fcdsAvroSchemaString);


        KafkaSource<Row> fcdsSource = KafkaSource.<Row>builder()
                .setBootstrapServers(brokers)
                .setTopics(fcdsTopic)
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
                .setValueOnlyDeserializer(fcdsRowSchema)
                .build();

        /* Question 11 */
        DataStream<Row> canStream = env.fromSource(canSource, WatermarkStrategy.noWatermarks(),"canSource")
//                .filter(row ->
////                        row.getFieldAs("trip_id").equals("f5637bd4-6d92-417d-8923-2994d079f17a")
////                        |
//                        row.getFieldAs("trip_id").equals("a3c7a23b-eaf3-4666-83e3-0bf0e1c40469")
//                )
                ;

        DataStream<Row> fcdsStream = env.fromSource(fcdsSource, WatermarkStrategy.noWatermarks(),"fcdsSource")
//                .filter(row ->
////                        row.getFieldAs("trip_id").equals("f5637bd4-6d92-417d-8923-2994d079f17a")
////                        |
//                        row.getFieldAs("trip_id").equals("a3c7a23b-eaf3-4666-83e3-0bf0e1c40469")
//                )
                ;
//
//        canStream.print();
//        fcdsStream.print();


//        canStream.keyBy(row -> row.getFieldAs("trip_id"))
//                .map(new MovingState())
//                .print();

        tableEnv.createTemporaryView("CanTable", canStream);
        tableEnv.createTemporaryView("FcdsTable", fcdsStream);


//        String query = "select c.source_timestamp, c.trip_id, c.dashboard_speed, c.gy, c.gx, f.functionalRoadClass, f.speedLimit " +
//                " from CanTable c, FcdsTable f " +
//                " WHERE c.trip_id = f.trip_id " +
//                " AND c.source_timestamp BETWEEN f.source_timestamp - INTERVAL '1' SECOND and f.source_timestamp + INTERVAL '1' SECOND"
//                ;

        Table historical = tableEnv.fromValues(
                DataTypes.ROW(
                        DataTypes.FIELD("functionalRoadClass", DataTypes.BIGINT()),
                        DataTypes.FIELD("mean", DataTypes.DECIMAL(10, 4)),
                        DataTypes.FIELD("stddev", DataTypes.DECIMAL(10, 4)),
                        DataTypes.FIELD("threshold", DataTypes.DECIMAL(10, 4))
                ),
                row(1.0,10406.013570921377, 2134.4733072089775, 16809.43349254831),
                row(2.0,5849.115507442035, 3397.098964466521, 16040.412400841598),
                row(3.0,4622.858000053575, 2226.4942218251654, 11302.340665529071),
                row(4.0,4050.1698013023574, 2014.0523913680272, 10092.326975406439),
                row(5.0,3226.014678740815, 2177.4663291716815, 9758.41366625586)
        );

        tableEnv.createTemporaryView("HistoricalData", historical);




        String queryJoin = "select c.source_timestamp, c.trip_id, c.dashboard_speed, c.gy, c.gx, f.functionalRoadClass, f.speedLimit " +
            " from CanTable c, FcdsTable f " +
            " WHERE c.trip_id = f.trip_id " +
            " AND EXTRACT(SECOND FROM c.source_timestamp)= EXTRACT(SECOND FROM f.source_timestamp)"
            ;

        Table joinTable = tableEnv.sqlQuery(
                queryJoin
        );
        tableEnv.createTemporaryView("JoinTable", joinTable);
        joinTable.printSchema();


        String queryJoinHist = "select j.trip_id, j.source_timestamp, j.dashboard_speed/100.0, j.functionalRoadClass, j.speedLimit, h.threshold," +
                " j.dashboard_speed - h.threshold as delta_speed," +
                " j.dashboard_speed < h.threshold as safe_speed, " +
                " j.dashboard_speed/100.0 - j.speedLimit as delta_speed_limit, " +
                " j.dashboard_speed/100.0 > j.speedLimit as overspeeding" +
                " from HistoricalData h, JoinTable j " +
                " WHERE j.functionalRoadClass = h.functionalRoadClass ";

//        String queryJoinHist = "select distinct functionalRoadClass from JoinTable";

        Table joinHistTable = tableEnv.sqlQuery(
                queryJoinHist
        );

        tableEnv.createTemporaryView("JoinHistTable", joinHistTable);

        DataStream<Row> question14_fromTable = tableEnv.toChangelogStream(joinHistTable);
        question14_fromTable.print();

        /* Question 15 */

//        String queryIsSage = "select j.trip_id, j.source_timestamp, j.dashboard_speed, h.threshold, j.dashboard_speed - h.threshold as delta_speed " +
//                " from HistoricalData h, JoinTable j " +
//                " WHERE j.functionalRoadClass = h.functionalRoadClass ";
//
////        String queryJoinHist = "select distinct functionalRoadClass from JoinTable";
//
//        Table joinHistTable = tableEnv.sqlQuery(
//                queryJoinHist
//        );


//        String queryAvgStd = "select functionalRoadClass, trip_id," +
//                " AVG(dashboard_speed), STDDEV(dashboard_speed)," +
//                " AVG(SQRT(POWER(gy, 2)+POWER(gx, 2))), STDDEV(SQRT(POWER(gy, 2)+POWER(gx, 2)))" +
//                " from JoinTable " +
//                " GROUP BY functionalRoadClass, trip_id "
//                ;
//        Table avgStdTable = tableEnv.sqlQuery(
//                queryAvgStd
//        );
//        DataStream<Row> question11_fromTable = tableEnv.toChangelogStream(avgStdTable);
//        question11_fromTable.print();

//        putInDB(question10_fromTable, config);

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