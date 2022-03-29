package eu.euranova.novhack;

import com.fasterxml.jackson.annotation.JsonProperty;

public class KafkaConfig {
    @JsonProperty("bootstrap.servers")
    private String bootstrapServers;

    @JsonProperty("group.id")
    private String groupId;

    @JsonProperty("auto.offset.reset")
    private String autoOffsetReset;

    @JsonProperty("security.protocol")
    private String securityProtocol;

    @JsonProperty("sasl.mechanism")
    private String saslMechanism;

    @JsonProperty("sasl.jaas.config")
    private String saslJaasConfig;

    @JsonProperty("sasl.client.callback.handler.class")
    private String saslClientCallbackHandlerClass;


    public String getSecurityProtocol() {
        return securityProtocol;
    }

    public String getAutoOffsetReset() {
        return autoOffsetReset;
    }

    public void setAutoOffsetReset(String autoOffsetReset) {
        this.autoOffsetReset = autoOffsetReset;
    }

    public String getBootstrapServers() {
        return bootstrapServers;
    }

    public void setBootstrapServers(String bootstrapServers) {
        this.bootstrapServers = bootstrapServers;
    }

    public String getGroupId() {
        return groupId;
    }

    public void setGroupId(String groupId) {
        this.groupId = groupId;
    }

    public String getSaslClientCallbackHandlerClass() {
        return saslClientCallbackHandlerClass;
    }

    public void setSaslClientCallbackHandlerClass(String saslClientCallbackHandlerClass) {
        this.saslClientCallbackHandlerClass = saslClientCallbackHandlerClass;
    }

    public String getSaslJaasConfig() {
        return saslJaasConfig;
    }

    public void setSaslJaasConfig(String saslJaasConfig) {
        this.saslJaasConfig = saslJaasConfig;
    }

    public String getSaslMechanism() {
        return saslMechanism;
    }

    public void setSaslMechanism(String saslMechanism) {
        this.saslMechanism = saslMechanism;
    }

    public void setSecurityProtocol(String securityProtocol) {
        this.securityProtocol = securityProtocol;
    }
}
