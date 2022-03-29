package eu.euranova.novhack;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
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
import java.time.LocalDateTime;
import java.util.Date;

import static org.apache.flink.table.api.Expressions.$;
import static org.apache.flink.table.api.Expressions.lit;


// import org.json.simple.JSONObject;
// import org.json.simple.parser.JSONParser;


public class SampleApplication {


        private static DataStream<Row> compute_speed(DataStream<Row> dataStream) {
            return dataStream.map(new MapFunction<Row, Row>() {
                @Override
                public Row map(Row row) throws Exception {
                    Double result = Double.valueOf(row.getFieldAs("dashboard_speed").toString()) / 100.0;

                    Row out = Row.withNames(row.getKind());
                    out.setField("timestamp", row.getFieldAs("timestamp"));
                    out.setField("trip_id", row.getFieldAs("trip_id"));
                    out.setField("res", result);

                    return out;

                }
            });
        }

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



            /* Question 1 */
            DataStream<Row> canStream = env.fromSource(source, WatermarkStrategy.noWatermarks(),"canSource");

            canStream.print();

            /* Using DataStream API */

//            DataStream<Row> question1_fromDataStream = compute_speed(canStream);
//            putInDB(question1_fromDataStream, config);

            /* Using Table API */

            tableEnv.createTemporaryView("mytable", canStream);
            Table mytable = tableEnv.sqlQuery("select `timestamp`, trip_id, CAST(dashboard_speed / 100.0 as DOUBLE) as res from mytable");
            DataStream<Row> question1_fromTable = tableEnv.toChangelogStream(mytable);

            putInDB(question1_fromTable, config);


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

            if (row.getFieldAs("timestamp") instanceof LocalDateTime) {
                LocalDateTime tmp = row.getFieldAs("timestamp");
                statement.setTimestamp(1, Timestamp.valueOf(tmp));
            }
            else {
                statement.setTimestamp(1, row.getFieldAs("timestamp"));
            }
            statement.setString(2, row.getFieldAs("trip_id"));
            statement.setDouble(3, row.getFieldAs("res"));
        };

        SinkFunction<Row> jdbcSink = JdbcSink.sink(
                "insert into speed (timestamp, trip_id, speed) values (?, ?, ?)",
                jdbcStatementBuilder,
                JdbcExecutionOptions.builder()
                        //                 .withBatchSize(200)
                        .withBatchIntervalMs(batchIntervalMs)
                        //                 .withMaxRetries(5)
                        .build(),
                new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                        .withUrl(URL)
                        .withDriverName("org.postgresql.Driver")
                        .withUsername(username)
                        .withPassword(password)
                        .build()
        );

        question_result
//                .print();
                .addSink(jdbcSink);

    }
}