package eu.euranova.novhack;

import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.connector.jdbc.JdbcStatementBuilder;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.types.Row;

import java.sql.Timestamp;
import java.time.LocalDateTime;

public class DBWriter {

    private final String url;
    private final String hostname;
    private final String username;
    private final Integer batchIntervalMs;
    private final String password;

    public DBWriter(JdbcConfig config) {
        this.hostname = config.getHostname();
        this.username = config.getUsername();
        this.batchIntervalMs = config.getBatchIntervalMs();
        this.password = config.getPassword();
        
        this.url = new StringBuffer().append("jdbc:postgresql://").append(this.hostname)
                .append("/").append(config.getDatabase()).toString();
    }

    public void sendingQuestions(DataStream<Row> question_result, String query, JdbcStatementBuilder<Row> jdbcStatementBuilder){
        SinkFunction<Row> jdbcSink = JdbcSink.sink(
                query,
                jdbcStatementBuilder,
                JdbcExecutionOptions.builder()
                        .withBatchIntervalMs(this.batchIntervalMs)
                        .build(),
                new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                        .withUrl(this.url)
                        .withDriverName("org.postgresql.Driver")
                        .withUsername(this.username)
                        .withPassword(this.password)
                        .build()
        );

        question_result.addSink(jdbcSink);
    }


    public void question1(DataStream<Row> question_result) {
        /* Query to insert data to speed table in DB  */
        String query = "insert into speed (timestamp, trip_id, speed) values (?, ?, ?)";
        JdbcStatementBuilder<Row> jdbcStatementBuilder = (statement, row) -> {

            if (row.getFieldAs("timestamp") instanceof LocalDateTime) {
                LocalDateTime tmp = row.getFieldAs("timestamp");
                statement.setTimestamp(1, Timestamp.valueOf(tmp));
            } else {
                statement.setTimestamp(1, row.getFieldAs("timestamp"));
            }
            statement.setString(2, row.getFieldAs("trip_id"));
            statement.setDouble(3, row.getFieldAs("res"));
        };

        sendingQuestions(question_result, query, jdbcStatementBuilder);

    }
}
