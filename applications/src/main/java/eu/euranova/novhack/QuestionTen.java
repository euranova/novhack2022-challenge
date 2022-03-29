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
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Date;

import static org.apache.flink.table.api.Expressions.$;
import static org.apache.flink.table.api.Expressions.call;


// import org.json.simple.JSONObject;
// import org.json.simple.parser.JSONParser;


public class QuestionTen {


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

        /* Question 10 */
        DataStream<Row> canStream = env.fromSource(source, WatermarkStrategy.noWatermarks(),"canSource")
                .filter(row -> row.getFieldAs("trip_id").equals("f5637bd4-6d92-417d-8923-2994d079f17a") |
                                row.getFieldAs("trip_id").equals("99e4781d-452b-4204-9224-a7e0256592d9")
                        );

//        canStream.print();


//        canStream.keyBy(row -> row.getFieldAs("trip_id"))
//                .map(new MovingState())
//                .print();

        tableEnv.createTemporaryView("MyTable", canStream);

        Table mytable = tableEnv
                .from("MyTable")
                .groupBy($("trip_id"))
                .select($("trip_id"), call(WeightedAvg.class, $("brake"), $("source_timestamp")));

        DataStream<Row> question10_fromTable = tableEnv.toChangelogStream(mytable);
        question10_fromTable.print();

//        putInDB(question10_fromTable, config);
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