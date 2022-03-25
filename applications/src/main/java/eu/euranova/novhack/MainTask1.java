package eu.euranova.novhack;

import com.amazonaws.services.kinesisanalytics.runtime.KinesisAnalyticsRuntime;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.formats.avro.AvroRowDeserializationSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class MainTask1 {

    private static final Logger log = LogManager.getLogger(MainTask1.class);

    public static Map<String, Properties> getKdaApplicationProperties() throws IOException {
        return KinesisAnalyticsRuntime.getApplicationProperties();
    }

    public static Map<String, Properties> getLocalApplicationProperties() throws IOException {

        InputStream propsIs = MainTask1.class.getClassLoader().getResourceAsStream("application.properties");
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

        /* Flink Environment */
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        /* Get configs */

        /* /!\ CHANGE IT WHEN PUT IN PRODUCTION VS LOCAL  /!\  */
        // Properties props = getKdaApplicationProperties().get("submissionProperties");
        Properties props = getLocalApplicationProperties().get("submissionProperties");

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
        InputStream canIs = MainTask1.class.getClassLoader().getResourceAsStream("can.avsc");
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

        InputStream fcdsIs = MainTask1.class.getClassLoader().getResourceAsStream("fcds.avsc");
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
                .filter(row -> true);

        DataStream<Row> fcdsStream = env.fromSource(fcdsSource, WatermarkStrategy.noWatermarks(),"fcdsSource")
                .filter(row -> true);

        /* This is where you start coding */

        // This are 2 example for doing Task 1. The outputs are equivalent.
        // You can comment one of the 2
        DataStream<Row> outputTableAPI = Question1.run_with_table(canStream, tableEnv);
        // DataStream<Row> outputDataStream = Task1.run_with_datastream(canStream, env);

        DBWriter dbWriter = new DBWriter(jdbcConfig);

        dbWriter.question1(outputTableAPI);
        // dbWriter.question1(outputDataStream);


        /* Execute Flink Env */
        env.execute();

    }

}
