package eu.euranova.novhack;

import com.fasterxml.jackson.annotation.JsonProperty;

public class JdbcConfig {
    private String hostname;
    private String username;
    private String password;
    private String database;

    @JsonProperty("batch_interval_ms")
    private Integer batchIntervalMs;
    
    public String getHostname() {
        return hostname;
    }
    public Integer getBatchIntervalMs() {
        return batchIntervalMs;
    }
    public void setBatchIntervalMs(Integer batchIntervalMs) {
        this.batchIntervalMs = batchIntervalMs;
    }
    public String getDatabase() {
        return database;
    }
    public void setDatabase(String database) {
        this.database = database;
    }
    public String getPassword() {
        return password;
    }
    public void setPassword(String password) {
        this.password = password;
    }
    public String getUsername() {
        return username;
    }
    public void setUsername(String username) {
        this.username = username;
    }
    public void setHostname(String hostname) {
        this.hostname = hostname;
    }
}
