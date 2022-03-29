package eu.euranova.novhack;

import com.amazonaws.services.kinesisanalytics.runtime.KinesisAnalyticsRuntime;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple3;
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
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import static org.apache.flink.table.api.Expressions.*;


public class AllQuestionsInOne {

    private static final Logger log = LogManager.getLogger(AllQuestionsInOne.class);

    public static Map<String, Properties> getKdaApplicationProperties() throws IOException {
        return KinesisAnalyticsRuntime.getApplicationProperties();
    }

    public static Map<String, Properties> getLocalApplicationProperties() throws IOException {
        InputStream propsIs = AllQuestionsInOne.class.getClassLoader().getResourceAsStream("application.properties");
        Properties props = new Properties();
        props.load(propsIs);
        String groupId = String.valueOf(new Date().getTime());
        props.put("kafka.group.id", groupId);
        Map<String, Properties> propsMap = new HashMap<>();
        propsMap.put("submissionProperties", props);
        return propsMap;
    }


    public static void main(String[] args) throws Exception {
        log.info("This is how we log. Do *not* add a log entry for every event");
        Properties props = getKdaApplicationProperties().get("submissionProperties");
//        Properties props = getLocalApplicationProperties().get("submissionProperties");

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);


        /* Get configs */
        String brokers = props.getProperty("kafka.bootstrap.servers");
        String canTopic = props.getProperty("kafka.can.topic");
        String fcdsTopic = props.getProperty("kafka.fcds.topic");

        JdbcConfig jdbcConfig = new JdbcConfig();
        jdbcConfig.setDatabase(props.getProperty("jdbc.database"));
        jdbcConfig.setHostname(props.getProperty("jdbc.hostname"));
        jdbcConfig.setUsername(props.getProperty("jdbc.username"));
        jdbcConfig.setPassword(props.getProperty("jdbc.password"));
        jdbcConfig.setBatchIntervalMs(Integer.valueOf(props.getProperty("jdbc.batch.interval.ms")));

        /* KafkaSource : get streaming of data */
        InputStream canIs = AllQuestionsInOne.class.getClassLoader().getResourceAsStream("can.avsc");
        String canAvroSchemaString = new String(canIs.readAllBytes(), StandardCharsets.UTF_8);
        AvroRowDeserializationSchema canRowSchema = new AvroRowDeserializationSchema(canAvroSchemaString);

        KafkaSource<Row> canSource = KafkaSource.<Row>builder()
                .setBootstrapServers(brokers)
                .setTopics(canTopic)
                .setGroupId(props.getProperty("kafka.group.id"))
                .setProperty("auto.offset.reset", props.getProperty("kafka.auto.offset.reset"))
                .setProperty("security.protocol", props.getProperty("kafka.security.protocol"))
                .setProperty("sasl.mechanism", props.getProperty("kafka.sasl.mechanism"))
                .setProperty("sasl.jaas.config", props.getProperty("kafka.sasl.jaas.config"))
                .setProperty("sasl.client.callback.handler.class", props.getProperty("kafka.sasl.client.callback.handler.class"))
                .setValueOnlyDeserializer(canRowSchema)
                .build();


        InputStream fcdsIs = AllQuestionsInOne.class.getClassLoader().getResourceAsStream("fcds.avsc");
        String fcdsAvroSchemaString = new String(fcdsIs.readAllBytes(), StandardCharsets.UTF_8);
        AvroRowDeserializationSchema fcdsRowSchema = new AvroRowDeserializationSchema(fcdsAvroSchemaString);


        KafkaSource<Row> fcdsSource = KafkaSource.<Row>builder()
                .setBootstrapServers(brokers)
                .setTopics(fcdsTopic)
                .setGroupId(props.getProperty("kafka.group.id"))
                .setProperty("auto.offset.reset", props.getProperty("kafka.auto.offset.reset"))
                .setProperty("security.protocol", props.getProperty("kafka.security.protocol"))
                .setProperty("sasl.mechanism", props.getProperty("kafka.sasl.mechanism"))
                .setProperty("sasl.jaas.config", props.getProperty("kafka.sasl.jaas.config"))
                .setProperty("sasl.client.callback.handler.class", props.getProperty("kafka.sasl.client.callback.handler.class"))
                .setValueOnlyDeserializer(fcdsRowSchema)
                .build();

        DataStream<Row> canStream = env.fromSource(canSource, WatermarkStrategy.noWatermarks(), "canSource")
                .filter(row ->
                        true
//                        row.getFieldAs("trip_id").equals("f5637bd4-6d92-417d-8923-2994d079f17a")
//                        |
//                        row.getFieldAs("trip_id").equals("a3c7a23b-eaf3-4666-83e3-0bf0e1c40469")
                )
                ;
        DataStream<Row> fcdsStream = env.fromSource(fcdsSource, WatermarkStrategy.noWatermarks(),"fcdsSource")
                .filter(row ->
                        true
//                                row.getFieldAs("trip_id").equals("f5637bd4-6d92-417d-8923-2994d079f17a")
//                        |
//                        row.getFieldAs("trip_id").equals("a3c7a23b-eaf3-4666-83e3-0bf0e1c40469")
                );
//        canStream.print();
//        fcdsStream.print();

        /* Question 1 */


        tableEnv.createTemporaryView("CanTable", canStream);
        tableEnv.createTemporaryView("FcdsTable", fcdsStream);

        Table question1_mytable = tableEnv.sqlQuery("select `timestamp`, trip_id, CAST(dashboard_speed / 100.0 as DOUBLE) as res from CanTable");
        DataStream<Row> question1 = tableEnv.toChangelogStream(question1_mytable);
        putInDB(question1, jdbcConfig, "question1");

        /* Question 2 */

        DataStream<Row> question2 = canStream
                .keyBy(value -> value.getFieldAs("trip_id"))
                .flatMap(new MovingState("trip_id", "gy", "timestamp"));

        putInDB(question2, jdbcConfig, "question2");

//        question2.print();

        /* Question 3 */

        DataStream<Row> question3 = canStream
                .keyBy(value -> value.getFieldAs("trip_id"))
                .flatMap(new MovingState("trip_id", "gx", "timestamp"));

        putInDB(question3, jdbcConfig, "question3");

        /* Question 4 */
        DataStream<Row> question4 = canStream
                .keyBy(value -> value.getFieldAs("trip_id"))
                .flatMap(new MovingState("trip_id", "dashboard_speed", "timestamp"))
                .returns(Types.ROW_NAMED(
                        new String[]{"trip_id", "timestamp", "res"},
                        Types.STRING,
                        Types.SQL_TIMESTAMP,
                        Types.DOUBLE
                ));

        putInDB(question4, jdbcConfig, "question4");


        /* Question 5 */

        Table question5_mytable = tableEnv.sqlQuery("select `timestamp`, trip_id, POWER(dashboard_speed, 2) / gy as res from CanTable");
        DataStream<Row> question5 = tableEnv.toChangelogStream(question5_mytable);
//        putInDB(question5, jdbcConfig, "question5");


        /* Question 6 */
        DataStream<Row> question6 = canStream
                .keyBy(value -> value.getFieldAs("trip_id"))
                .flatMap(new MovingState("trip_id", "ecozone", "timestamp"));

//        putInDB(question6, jdbcConfig, "question6");

        /* Question 7 */
        DataStream<Row> question7 = canStream
                .keyBy(value -> value.getFieldAs("trip_id"))
                .flatMap(new MovingState("trip_id", "steering_angle", "timestamp"));

//        putInDB(question7, jdbcConfig, "question7");

        /* Question 8 */

        // Not Done because of Issue with Windows

        /* Question 9 */

        Table question9_mytable = tableEnv.sqlQuery(
                "select AVG(Cast(ecozone as DOUBLE)) as res, MAX(`timestamp`) as `timestamp` , trip_id from CanTable " +
                        "GROUP BY trip_id"
        );

//        question9_mytable.printSchema();

        DataStream<Row> question9 = tableEnv.toChangelogStream(question9_mytable);

//        putInDB(question9, jdbcConfig, "question9");


        /* Question 10 */
        // return also starting brake
        Table question10_mytable = tableEnv
                .from("CanTable")
                .groupBy($("trip_id"))
                .select($("trip_id"), call(WeightedAvg.class, $("brake"), $("timestamp")).as("res"))
                .where($("res").isNotNull());


        DataStream<Row> question10 = tableEnv.toChangelogStream(question10_mytable);


//        putInDB(question10, jdbcConfig, "question10");


        /* Question 11-12-13-14-15-16 SPEED */

        Table historical = tableEnv.fromValues(
                DataTypes.ROW(
                        DataTypes.FIELD("road_type", DataTypes.BIGINT()),

                        DataTypes.FIELD("speed_average", DataTypes.DOUBLE()),
                        DataTypes.FIELD("speed_stdev", DataTypes.DOUBLE()),
                        DataTypes.FIELD("speed_th", DataTypes.DOUBLE()),

                        DataTypes.FIELD("acceleration_average", DataTypes.DOUBLE()),
                        DataTypes.FIELD("acceleration_stdev", DataTypes.DOUBLE()),
                        DataTypes.FIELD("acceleration_th", DataTypes.DOUBLE()),

                        DataTypes.FIELD("deceleration_average", DataTypes.DOUBLE()),
                        DataTypes.FIELD("deceleration_stdev", DataTypes.DOUBLE()),
                        DataTypes.FIELD("deceleration_th", DataTypes.DOUBLE()),

                        DataTypes.FIELD("jerk_average", DataTypes.DOUBLE()),
                        DataTypes.FIELD("jerk_stdev", DataTypes.DOUBLE()),
                        DataTypes.FIELD("jerk_th", DataTypes.DOUBLE())

                ),
                row(1.0,
                        10406.013570921377, 2134.4733072089775, 16809.43349254831,
                        10406.013570921377, 2134.4733072089775, 16809.43349254831,
                        10406.013570921377, 2134.4733072089775, 16809.43349254831,
                        10406.013570921377, 2134.4733072089775, 16809.43349254831
                ),
                row(2.0, 5849.115507442035, 3397.098964466521, 16040.412400841598,
                        10406.013570921377, 2134.4733072089775, 16809.43349254831,
                        10406.013570921377, 2134.4733072089775, 16809.43349254831,
                        10406.013570921377, 2134.4733072089775, 16809.43349254831),
                row(3.0, 4622.858000053575, 2226.4942218251654, 11302.340665529071,
                        10406.013570921377, 2134.4733072089775, 16809.43349254831,
                        10406.013570921377, 2134.4733072089775, 16809.43349254831,
                        10406.013570921377, 2134.4733072089775, 16809.43349254831),
                row(4.0, 4050.1698013023574, 2014.0523913680272, 10092.326975406439,
                        10406.013570921377, 2134.4733072089775, 16809.43349254831,
                        10406.013570921377, 2134.4733072089775, 16809.43349254831,
                        10406.013570921377, 2134.4733072089775, 16809.43349254831),
                row(5.0, 3226.014678740815, 2177.4663291716815, 9758.41366625586,
                        10406.013570921377, 2134.4733072089775, 16809.43349254831,
                        10406.013570921377, 2134.4733072089775, 16809.43349254831,
                        10406.013570921377, 2134.4733072089775, 16809.43349254831)
        );

        DataStream<Row> historicalStream = tableEnv.toChangelogStream(historical);
//        putInDB(historicalStream, jdbcConfig, "question11");
//        putInDB(historicalStream, jdbcConfig, "question12");


        tableEnv.createTemporaryView("HistoricalData", historical);

//        String queryJoin = "select c.`timestamp`, f.`timestamp`, c.trip_id, c.dashboard_speed, c.gy, c.gx, f.functionalRoadClass, f.speedLimit " +
//                " from CanTable c, FcdsTable f " +
//                " WHERE c.trip_id = f.trip_id " +
//                " AND CAST(c.`timestamp` AS TIMESTAMP_LTZ(0))  = CAST(f.`timestamp` AS TIMESTAMP_LTZ(0))"
//                ;

        String queryJoin = "select c.`timestamp`, f.`timestamp`, c.trip_id, c.dashboard_speed, c.gy, c.gx, f.functionalRoadClass, f.speedLimit " +
                " from CanTable c" +
                " LEFT JOIN FcdsTable f " +
                " ON c.trip_id = f.trip_id " +
                " AND CAST(c.`timestamp` AS TIMESTAMP_LTZ(0)) = CAST(f.`timestamp` AS TIMESTAMP_LTZ(0))";

//        String FcdsGreater = "select f.`timestamp` from FcdsTable f order by f.`timestamp` desc limit 1";
//
//        // c.timestamp = (SELECT TOP 1 timestamp FROM c WHERE c.timestamp > f.timestamp order by f.timestamp ASC)
//
//        Table fcdsGreaterTable = tableEnv.sqlQuery(
//                FcdsGreater
//        );
//        tableEnv.createTemporaryView("FcdsGreaterTable", FcdsGreater);


//        String queryJoin = "select c.`timestamp`, f.`timestamp`, c.trip_id, c.dashboard_speed, c.gy, c.gx, f.functionalRoadClass, f.speedLimit " +
//                " from CanTable c" +
//                " LEFT JOIN FcdsTable f " +
//                " ON c.trip_id = f.trip_id " +
//                " AND c.`timestamp` = (SELECT topper.`timestamp` FROM (SELECT * from c WHERE c.`timestamp` > f.`timestamp` order by f.`timestamp` ASC LIMIT 1) as topper)"
//                ;

        // c.timestamp = (SELECT TOP 1 timestamp FROM c WHERE c.timestamp > f.timestamp order by f.timestamp ASC)

        Table joinTable = tableEnv.sqlQuery(
                queryJoin
        );

        tableEnv.createTemporaryView("JoinTable", joinTable);
//        joinTable.printSchema();


        String queryJoinHist = "select j.trip_id, j.`timestamp`, j.dashboard_speed/100.0, j.functionalRoadClass, j.speedLimit," +
                " h.road_type," +
                " j.dashboard_speed - h.speed_th as delta_speed," +
                " j.dashboard_speed < h.speed_th as safe_speed, " +
                " CAST(j.dashboard_speed/100.0 - j.speedLimit as DOUBLE) as delta_speed_limit, " +
                " j.dashboard_speed/100.0 > j.speedLimit as overspeeding " +
                " from HistoricalData h, JoinTable j " +
                " WHERE j.functionalRoadClass = h.road_type ";

        Table joinHistTable = tableEnv.sqlQuery(
                queryJoinHist
        );

        DataStream<Row> questionSpeedLimit = tableEnv.toChangelogStream(joinHistTable);

//        putInDB(questionSpeedLimit, jdbcConfig, "question15");
//        putInDB(questionSpeedLimit, jdbcConfig, "question16");

        tableEnv.createTemporaryView("JoinHistTable", joinHistTable);


//        question10To16Speed.print();

        /* Question 11-12-13-14-15-16 ACCELERATION*/


        tableEnv.createTemporaryView("Question4Table", question4);

        // TODO : change extract
//        String queryAccelerationJoin = "select c.`timestamp`, c.trip_id, c.res, f.functionalRoadClass " +
//                " from Question4Table c, FcdsTable f " +
//                " WHERE c.`timestamp` BETWEEN f.`timestamp` - INTERVAL '1' SECOND AND f.`timestamp`" +
//                " AND c.trip_id = f.trip_id"
//                ;

        String queryAccelerationJoin = "select c.`timestamp`, f.`timestamp`, c.res, c.trip_id, f.functionalRoadClass " +
                " from Question4Table c " +
                " LEFT JOIN FcdsTable f " +
                " ON c.trip_id = f.trip_id " +
                " AND CAST(c.`timestamp` AS TIMESTAMP_LTZ(0)) = CAST(f.`timestamp` AS TIMESTAMP_LTZ(0))";

//        String queryAccelerationJoin = "select c.`timestamp`, f.`timestamp`, c.trip_id, f.functionalRoadClass " +
//                " from Question4Table c " +
//                " LEFT JOIN FcdsTable f " +
//                " ON c.trip_id = f.trip_id " +
//                " AND c.timestamp = (SELECT TOP 1 timestamp FROM c WHERE c.timestamp > f.timestamp order by f.timestamp ASC)"
//                ;

        /*
        SELECT TOP 1 timestamp FROM c WHERE c.timestamp > f.timestamp order by f.timestamp ASC ;
         */

        Table joinAccelerationTable = tableEnv.sqlQuery(
                queryAccelerationJoin
        );

        tableEnv.createTemporaryView("JoinAccelerationTable", joinAccelerationTable);

        String queryAccelerationComputation = "select j.trip_id, j.`timestamp`, j.functionalRoadClass," +
                " h.road_type," +
                " j.res - h.acceleration_th as delta_acceleration," +
                " j.res < h.acceleration_th as safe_acceleration " +
                " from HistoricalData h, JoinAccelerationTable j " +
                " WHERE j.functionalRoadClass = h.road_type AND j.res > 0";

        Table AccelerationTable = tableEnv.sqlQuery(
                queryAccelerationComputation
        );

        tableEnv.createTemporaryView("AccelerationTable", AccelerationTable);


        String queryDecelerationComputation = "select j.trip_id, j.`timestamp`, j.functionalRoadClass," +
                " h.road_type," +
                " j.res - h.deceleration_th as delta_deceleration," +
                " j.res < h.deceleration_th as safe_deceleration " +
                " from HistoricalData h, JoinAccelerationTable j " +
                " WHERE j.functionalRoadClass = h.road_type AND j.res < 0";

        Table DecelerationTable = tableEnv.sqlQuery(
                queryDecelerationComputation
        );

        tableEnv.createTemporaryView("DecelerationTable", DecelerationTable);


        // DecelerationTable, AccelerationTable, JoinHistTable
        String queryJoinQ13 = "select j.trip_id, j.`timestamp`," +
                " j.delta_speed, a.delta_acceleration, d.delta_deceleration, " +
                " j.safe_speed, a.safe_acceleration, d.safe_deceleration " +
                " from JoinHistTable j " +
                " LEFT JOIN DecelerationTable d " +
                " ON j.trip_id = d.trip_id AND j.`timestamp` = d.`timestamp` " +
                " LEFT JOIN AccelerationTable a " +
                " ON j.trip_id = a.trip_id AND j.`timestamp` = a.`timestamp`";

        Table queryJoinQ13Table = tableEnv.sqlQuery(
                queryJoinQ13
        );

//        queryJoinQ13Table.printSchema();


        DataStream<Row> questionEnd = tableEnv.toChangelogStream(queryJoinQ13Table);
//        putInDB(questionEnd, jdbcConfig, "question13");
//        putInDB(questionEnd, jdbcConfig, "question14");


        env.execute();
    }

    private static void putInDB(DataStream<Row> question_result, JdbcConfig config, String question) {
        /* Sink */

        String hostname = config.getHostname();
        String database = config.getDatabase();
        String username = config.getUsername();
        int batchIntervalMs = config.getBatchIntervalMs();
        String password = config.getPassword();

        String URL = new StringBuffer().append("jdbc:postgresql://").append(hostname)
                .append("/").append(database).toString();

        String query;
        JdbcStatementBuilder<Row> jdbcStatementBuilder;

        switch (question) {
            case "question1":
                query = "insert into speed (timestamp, trip_id, speed) values (?, ?, ?)";
                jdbcStatementBuilder = (statement, row) -> {

                    if (row.getFieldAs("timestamp") instanceof LocalDateTime) {
                        LocalDateTime tmp = row.getFieldAs("timestamp");
                        statement.setTimestamp(1, Timestamp.valueOf(tmp));
                    } else {
                        statement.setTimestamp(1, row.getFieldAs("timestamp"));
                    }
                    statement.setString(2, row.getFieldAs("trip_id"));
                    statement.setDouble(3, row.getFieldAs("res"));
                };
                break;
            case "question2":
                query = "insert into lateral_jerk (timestamp, trip_id, lateral_jerk) values (?, ?, ?)";
                jdbcStatementBuilder = (statement, row) -> {

                    if (row.getFieldAs("timestamp") instanceof LocalDateTime) {
                        LocalDateTime tmp = row.getFieldAs("timestamp");
                        statement.setTimestamp(1, Timestamp.valueOf(tmp));
                    } else {
                        statement.setTimestamp(1, row.getFieldAs("timestamp"));
                    }
                    statement.setString(2, row.getFieldAs("trip_id"));
                    statement.setDouble(3, row.getFieldAs("res"));
                };
                break;
            case "question3":
                query = "insert into longitudinal_jerk (timestamp, trip_id, longitudinal_jerk) values (?, ?, ?)";
                jdbcStatementBuilder = (statement, row) -> {

                    if (row.getFieldAs("timestamp") instanceof LocalDateTime) {
                        LocalDateTime tmp = row.getFieldAs("timestamp");
                        statement.setTimestamp(1, Timestamp.valueOf(tmp));
                    } else {
                        statement.setTimestamp(1, row.getFieldAs("timestamp"));
                    }
                    statement.setString(2, row.getFieldAs("trip_id"));
                    statement.setDouble(3, row.getFieldAs("res"));
                };
                break;
            case "question4":
                query = "insert into acceleration (timestamp, trip_id, acceleration) values (?, ?, ?)";
                jdbcStatementBuilder = (statement, row) -> {

                    if (row.getFieldAs("timestamp") instanceof LocalDateTime) {
                        LocalDateTime tmp = row.getFieldAs("timestamp");
                        statement.setTimestamp(1, Timestamp.valueOf(tmp));
                    } else {
                        statement.setTimestamp(1, row.getFieldAs("timestamp"));
                    }
                    statement.setString(2, row.getFieldAs("trip_id"));
                    statement.setDouble(3, row.getFieldAs("res"));
                };
                break;
            case "question5":
                query = "insert into road_curvature (timestamp, trip_id, road_curvature) values (?, ?, ?)";
                jdbcStatementBuilder = (statement, row) -> {

                    if (row.getFieldAs("timestamp") instanceof LocalDateTime) {
                        LocalDateTime tmp = row.getFieldAs("timestamp");
                        statement.setTimestamp(1, Timestamp.valueOf(tmp));
                    } else {
                        statement.setTimestamp(1, row.getFieldAs("timestamp"));
                    }
                    statement.setString(2, row.getFieldAs("trip_id"));
                    statement.setDouble(3, row.getFieldAs("res"));
                };
                break;
            case "question6":
                query = "insert into eco_zone_variation (timestamp, trip_id, eco_zone_variation) values (?, ?, ?)";
                jdbcStatementBuilder = (statement, row) -> {

                    if (row.getFieldAs("timestamp") instanceof LocalDateTime) {
                        LocalDateTime tmp = row.getFieldAs("timestamp");
                        statement.setTimestamp(1, Timestamp.valueOf(tmp));
                    } else {
                        statement.setTimestamp(1, row.getFieldAs("timestamp"));
                    }
                    statement.setString(2, row.getFieldAs("trip_id"));
                    statement.setDouble(3, row.getFieldAs("res"));
                };
                break;
            case "question7":
                query = "insert into steering_angle_variation (timestamp, trip_id, steering_angle_variation) values (?, ?, ?)";
                jdbcStatementBuilder = (statement, row) -> {

                    if (row.getFieldAs("timestamp") instanceof LocalDateTime) {
                        LocalDateTime tmp = row.getFieldAs("timestamp");
                        statement.setTimestamp(1, Timestamp.valueOf(tmp));
                    } else {
                        statement.setTimestamp(1, row.getFieldAs("timestamp"));
                    }
                    statement.setString(2, row.getFieldAs("trip_id"));
                    statement.setDouble(3, row.getFieldAs("res"));
                };
                break;
            case "question9":
                query = "insert into moving_average (timestamp, trip_id, moving_average) values (?, ?, ?)";
                jdbcStatementBuilder = (statement, row) -> {

                    if (row.getFieldAs("timestamp") instanceof LocalDateTime) {
                        LocalDateTime tmp = row.getFieldAs("timestamp");
                        statement.setTimestamp(1, Timestamp.valueOf(tmp));
                    } else {
                        statement.setTimestamp(1, row.getFieldAs("timestamp"));
                    }
                    statement.setString(2, row.getFieldAs("trip_id"));
                    statement.setDouble(3, row.getFieldAs("res"));
                };
                break;
            case "question10":
                query = "insert into average_braking_time (begin_timestamp, trip_id, average_braking_time, brake_delta_time) values (?, ?, ?, ?)";
                jdbcStatementBuilder = (statement, row) -> {

                    statement.setString(2, row.getFieldAs("trip_id"));

                    Tuple3<Timestamp, Double, Long> metadata = row.getFieldAs("res");

                    statement.setTimestamp(1, metadata.f0);
                    statement.setDouble(3, metadata.f1);
                    statement.setLong(4, metadata.f2);
                };
                break;
            case "question11":
                query = "insert into stats_road_type (road_type, speed_average, speed_stdev, " +
                        " acceleration_average, acceleration_stdev, " +
                        " deceleration_average, deceleration_stdev, " +
                        " jerk_average, jerk_stdev)" +
                        " values (?, ?, ?, ?, ?, ?, ?, ?, ?)";
                jdbcStatementBuilder = (statement, row) -> {

                    statement.setString(1, row.getFieldAs("road_type").toString());

//                    double speed_average = ((BigDecimal) row.getFieldAs("speed_average")).doubleValue();
//                    double speed_stdev = ((BigDecimal) row.getFieldAs("speed_stdev")).doubleValue();
//                    double acceleration_average = ((BigDecimal) row.getFieldAs("acceleration_average")).doubleValue();
//                    double acceleration_stdev = ((BigDecimal) row.getFieldAs("acceleration_stdev")).doubleValue();
//                    double deceleration_average = ((BigDecimal) row.getFieldAs("deceleration_average")).doubleValue();
//                    double deceleration_stdev = ((BigDecimal) row.getFieldAs("deceleration_stdev")).doubleValue();
//                    double jerk_average = ((BigDecimal) row.getFieldAs("jerk_average")).doubleValue();
//                    double jerk_stdev = ((BigDecimal) row.getFieldAs("jerk_stdev")).doubleValue();

                    statement.setDouble(2, row.getFieldAs("speed_average"));
                    statement.setDouble(3, row.getFieldAs("speed_stdev"));

                    statement.setDouble(4, row.getFieldAs("acceleration_average"));
                    statement.setDouble(5, row.getFieldAs("acceleration_stdev"));

                    statement.setDouble(6, row.getFieldAs("deceleration_average"));
                    statement.setDouble(7, row.getFieldAs("deceleration_stdev"));

                    statement.setDouble(8, row.getFieldAs("jerk_average"));
                    statement.setDouble(9, row.getFieldAs("jerk_stdev"));

                };
                break;
            case "question12":
                query = "insert into thresholds (road_type, speed_th, acceleration_th, " +
                        " deceleration_th, jerk_th) " +
                        " values (?, ?, ?, ?, ?)";
                jdbcStatementBuilder = (statement, row) -> {

                    statement.setString(1, row.getFieldAs("road_type").toString());

                    statement.setDouble(2, row.getFieldAs("speed_th"));
                    statement.setDouble(3, row.getFieldAs("acceleration_th"));

                    statement.setDouble(4, row.getFieldAs("deceleration_th"));
                    statement.setDouble(5, row.getFieldAs("jerk_th"));

                };
                break;
            case "question13":
                query = "insert into differences (timestamp, trip_id, delta_speed, " +
                        " delta_acceleration, delta_deceleration, delta_jerk) " +
                        " values (?, ?, ?, ?, ?, ?)";
                jdbcStatementBuilder = (statement, row) -> {

                    if (row.getFieldAs("timestamp") instanceof LocalDateTime) {
                        LocalDateTime tmp = row.getFieldAs("timestamp");
                        statement.setTimestamp(1, Timestamp.valueOf(tmp));
                    } else {
                        statement.setTimestamp(1, row.getFieldAs("timestamp"));
                    }
                    statement.setString(2, row.getFieldAs("trip_id"));
                    statement.setDouble(3, row.getFieldAs("delta_speed"));

                    Double delta_acceleration = row.getFieldAs("delta_acceleration");
                    if (delta_acceleration == null)
                        statement.setObject(4, null);
                    else
                        statement.setDouble(4, delta_acceleration);

                    Double delta_deceleration = row.getFieldAs("delta_deceleration");
                    if (delta_deceleration == null)
                        statement.setObject(5, null);
                    else
                        statement.setDouble(5, delta_deceleration);

                    statement.setDouble(6, -1.0);


                };
                break;
            case "question14":
                query = "insert into is_safe (timestamp, trip_id, safe_speed, " +
                        " safe_acceleration, safe_deceleration, safe_jerk, is_safe) " +
                        " values (?, ?, ?, ?, ?, ?, ?)";
                jdbcStatementBuilder = (statement, row) -> {

                    Object safe_speed = row.getFieldAs("safe_speed");
                    Object safe_acceleration = row.getFieldAs("safe_acceleration");
                    Object safe_deceleration = row.getFieldAs("safe_deceleration");
                    int isSafe = 0;
                    int myInt;

                    if (row.getFieldAs("timestamp") instanceof LocalDateTime) {
                        LocalDateTime tmp = row.getFieldAs("timestamp");
                        statement.setTimestamp(1, Timestamp.valueOf(tmp));
                    } else {
                        statement.setTimestamp(1, row.getFieldAs("timestamp"));
                    }
                    statement.setString(2, row.getFieldAs("trip_id"));

                    if (safe_speed == null) {
                        myInt = 0;
                        statement.setObject(3, safe_speed);
                    } else {
                        myInt = ((boolean) safe_speed) ? 1 : 0;
                        isSafe = isSafe + myInt;
                        statement.setBoolean(3, (boolean) safe_speed);
                    }
                    if (safe_acceleration == null) {
                        myInt = 0;
                        statement.setObject(4, safe_acceleration);
                    } else {
                        myInt = ((boolean) safe_acceleration) ? 1 : 0;
                        isSafe = isSafe + myInt;
                        statement.setBoolean(4, (boolean) safe_acceleration);
                    }

                    if (safe_deceleration == null) {
                        myInt = 0;
                        statement.setObject(5, safe_deceleration);
                    } else {
                        myInt = ((boolean) safe_deceleration) ? 1 : 0;
                        isSafe = isSafe + myInt;
                        statement.setBoolean(5, (boolean) safe_deceleration);
                    }

                    statement.setBoolean(6, false);

                    statement.setBoolean(7, isSafe == 3);


                };
                break;
            case "question15":
                query = "insert into delta_speed_limit (timestamp, trip_id, delta_speed_limit)" +
                        " values (?, ?, ?)";
                jdbcStatementBuilder = (statement, row) -> {

                    if (row.getFieldAs("timestamp") instanceof LocalDateTime) {
                        LocalDateTime tmp = row.getFieldAs("timestamp");
                        statement.setTimestamp(1, Timestamp.valueOf(tmp));
                    } else {
                        statement.setTimestamp(1, row.getFieldAs("timestamp"));
                    }
                    statement.setString(2, row.getFieldAs("trip_id"));
                    statement.setDouble(3, row.getFieldAs("delta_speed_limit"));

                };
                break;
            case "question16":
                query = "insert into overspeeding (timestamp, trip_id, overspeeding)" +
                        " values (?, ?, ?)";
                jdbcStatementBuilder = (statement, row) -> {

                    if (row.getFieldAs("timestamp") instanceof LocalDateTime) {
                        LocalDateTime tmp = row.getFieldAs("timestamp");
                        statement.setTimestamp(1, Timestamp.valueOf(tmp));
                    } else {
                        statement.setTimestamp(1, row.getFieldAs("timestamp"));
                    }
                    statement.setString(2, row.getFieldAs("trip_id"));
                    statement.setBoolean(3, row.getFieldAs("overspeeding"));

                };
                break;
            default:
                jdbcStatementBuilder = null;
                query = "";
                System.out.println("NotImplemented String question");

        }
        SinkFunction<Row> jdbcSink = JdbcSink.sink(
                query,
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

//        question_result.print();
        question_result.addSink(jdbcSink);

    }
}
