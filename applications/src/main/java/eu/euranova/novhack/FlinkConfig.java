package eu.euranova.novhack;

import com.fasterxml.jackson.annotation.JsonProperty;

public class FlinkConfig {
    private KafkaConfig kafka;

    @JsonProperty("aws_profile")
    private String awsProfile;

    private String region;

    private String jar;

    @JsonProperty("input_topics")
    private InputTopics inputTopics;
    
    private JdbcConfig jdbc;

    public KafkaConfig getKafka() {
        return kafka;
    }

    public JdbcConfig getJdbc() {
        return jdbc;
    }

    public void setJdbc(JdbcConfig jdbc) {
        this.jdbc = jdbc;
    }

    public InputTopics getInputTopics() {
        return inputTopics;
    }

    public void setInputTopics(InputTopics inputTopics) {
        this.inputTopics = inputTopics;
    }

    public String getJar() {
        return jar;
    }

    public void setJar(String jar) {
        this.jar = jar;
    }

    public String getRegion() {
        return region;
    }

    public void setRegion(String region) {
        this.region = region;
    }

    public String getAwsProfile() {
        return awsProfile;
    }

    public void setAwsProfile(String awsProfile) {
        this.awsProfile = awsProfile;
    }

    public void setKafka(KafkaConfig kafka) {
        this.kafka = kafka;
    }
}
