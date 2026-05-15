package org.n52.edis.pegelonlinekafkaconsumer.mqtt;

import com.fasterxml.jackson.core.JacksonException;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.n52.edis.pegelonlinekafkaconsumer.model.PegelonlineMqttMessage;
import org.n52.edis.pegelonlinekafkaconsumer.model.PegelonlineTopic;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public abstract class AbstractMqttPublisher {

    private static final Logger LOGGER = LoggerFactory.getLogger(AbstractMqttPublisher.class);

    private List<String> serverUris;
    private String clientIdPrefix;

    private String clientId;

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

    public List<String> getServerUris() {
        return serverUris;
    }

    public void setServerUris(List<String> serverUris) {
        this.serverUris = serverUris;
    }

    public String getClientIdPrefix() {
        return clientIdPrefix;
    }

    public void setClientIdPrefix(String clientIdPrefix) {
        this.clientIdPrefix = clientIdPrefix;
    }

    public String getClientId() {
        return clientId;
    }

    public void setClientId(String clientId) {
        this.clientId = clientId;
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

    public abstract void connect() throws MqttException;

    public abstract boolean isConnected();

    public abstract void publishMessage(PegelonlineMqttMessage payload, PegelonlineTopic topic)
            throws JacksonException;

    protected void logConfiguration() {
        LOGGER.debug("Server URI: {}", getServerUris());
        LOGGER.debug("Base topic: {}", getBaseTopic());
        LOGGER.debug("Connection timeout: {}", getConnectionTimeout());
        LOGGER.debug("Client ID prefix: {}", getClientIdPrefix());
        LOGGER.debug("Client ID: {}", getClientId());
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
