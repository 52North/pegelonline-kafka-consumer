package org.n52.edis.pegelonlinekafkaconsumer.mqtt;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.context.properties.ConfigurationProperties;

public abstract class AbstractMqttPublisher {

    private static final Logger LOGGER = LoggerFactory.getLogger(AbstractMqttPublisher.class);

    private String serverUri;
    private String clientIdPrefix;

    private String baseTopic;
    private boolean filePersistenceEnabled;
    private String filePersistenceDirectory;
    private boolean cleanSession;
    private boolean reconnect;
    private int connectionTimeout;
    private int keepAliveInterval;
    private int qos;
    private boolean retained;
    private boolean basicAuthentication;

    private String username;
    private String password;


    public String getServerUri() {
        return serverUri;
    }

    public void setServerUri(String serverUri) {
        this.serverUri = serverUri;
    }

    public String getClientIdPrefix() {
        return clientIdPrefix;
    }

    public void setClientIdPrefix(String clientIdPrefix) {
        this.clientIdPrefix = clientIdPrefix;
    }



    public boolean isFilePersistenceEnabled() {
        return filePersistenceEnabled;
    }

    public void setFilePersistenceEnabled(boolean filePersistenceEnabled) {
        this.filePersistenceEnabled = filePersistenceEnabled;
    }

    public String getFilePersistenceDirectory() {
        return filePersistenceDirectory;
    }

    public void setFilePersistenceDirectory(String filePersistenceDirectory) {
        this.filePersistenceDirectory = filePersistenceDirectory;
    }


    public boolean isCleanSession() {
        return cleanSession;
    }

    public void setCleanSession(boolean cleanSession) {
        this.cleanSession = cleanSession;
    }

    public boolean isReconnect() {
        return reconnect;
    }

    public void setReconnect(boolean reconnect) {
        this.reconnect = reconnect;
    }

    public int getConnectionTimeout() {
        return connectionTimeout;
    }

    public void setConnectionTimeout(int connectionTimeout) {
        this.connectionTimeout = connectionTimeout;
    }

    public int getKeepAliveInterval() {
        return keepAliveInterval;
    }

    public void setKeepAliveInterval(int keepAliveInterval) {
        this.keepAliveInterval = keepAliveInterval;
    }

    public String getBaseTopic() {
        return baseTopic;
    }

    public void setBaseTopic(String baseTopic) {
        this.baseTopic = baseTopic;
    }

    public int getQos() {
        return qos;
    }

    public void setQos(int qos) {
        this.qos = qos;
    }

    public boolean isRetained() {
        return retained;
    }

    public void setRetained(boolean retained) {
        this.retained = retained;
    }

    public boolean isBasicAuthentication() {
        return basicAuthentication;
    }

    public void setBasicAuthentication(boolean basicAuthentication) {
        this.basicAuthentication = basicAuthentication;
    }

    public String getUsername() {
        return username;
    }

    public void setUsername(String username) {
        this.username = username;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    protected void logConfiguration() {
        LOGGER.debug("Server URI: {}", getServerUri());
        LOGGER.debug("Base topic: {}", getBaseTopic());
        LOGGER.debug("Connection timeout: {}", getConnectionTimeout());
        LOGGER.debug("Client ID prefix: {}", getClientIdPrefix());
        LOGGER.debug("File persistence enabled: {}", isFilePersistenceEnabled());
        LOGGER.debug("File persistence directory: {}", getFilePersistenceDirectory());
        LOGGER.debug("Clean session: {}", isCleanSession());
        LOGGER.debug("Reconnect: {}", isReconnect());
        LOGGER.debug("Keep alive interval: {}", getKeepAliveInterval());
        LOGGER.debug("Qos: {}", getQos());
        LOGGER.debug("Retained: {}", isRetained());
        LOGGER.debug("Basic-Authentication: {}", isBasicAuthentication());
    }
}
