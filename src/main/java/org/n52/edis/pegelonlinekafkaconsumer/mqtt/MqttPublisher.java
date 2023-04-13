package org.n52.edis.pegelonlinekafkaconsumer.mqtt;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.eclipse.paho.client.mqttv3.*;
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence;
import org.eclipse.paho.client.mqttv3.persist.MqttDefaultFilePersistence;
import org.n52.edis.pegelonlinekafkaconsumer.model.PegelonlineMqttMessage;
import org.n52.edis.pegelonlinekafkaconsumer.model.PegelonlineTopic;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.retry.annotation.Backoff;
import org.springframework.retry.annotation.Recover;
import org.springframework.retry.annotation.Retryable;
import org.springframework.scheduling.annotation.Async;

import java.util.UUID;

public class MqttPublisher extends AbstractMqttPublisher implements MqttCallback, InitializingBean, DisposableBean {

    private static final Logger LOGGER = LoggerFactory.getLogger(MqttPublisher.class);

    private ObjectMapper jsonMapper;

    private MqttClient mqttClient;

    private MqttConnectOptions mqttConnectOptions;

    private MqttMessageDeliveryMonitor monitor;

    public MqttPublisher() {
        monitor = new MqttMessageDeliveryLoggingMonitor();
    }

    public MqttPublisher(MqttMessageDeliveryMonitor monitor) {
        this.monitor = monitor;
    }

    @Async
    @Retryable(value = MqttException.class,
            maxAttemptsExpression = "${edis.mqtt.initial-connect-retry-attempts}",
            backoff = @Backoff(delayExpression = "${edis.mqtt.initial-connect-retry-delay}"))
    public void connect() throws MqttException {
        LOGGER.info("Trying to connect to MQTT broker...");
        try {
            mqttClient.connect(mqttConnectOptions);
        } catch (MqttException ex) {
            LOGGER.error("Could not connect to MQTT broker. Cause: {}", ex.getMessage());
            LOGGER.debug("Mqtt command failure.", ex);
            throw ex;
        }
        LOGGER.info("Connection to MQTT broker successfully established.");
    }

    @Recover
    void recover(MqttException ex) {
        LOGGER.error("No initial connection to MQTT broker could be established. No MQTT support so far.");
    }

    public boolean isConnected() {
        return mqttClient.isConnected();
    }

    public void publishMessage(PegelonlineMqttMessage payload, PegelonlineTopic topic) throws JsonProcessingException {
        MqttMessage message = new MqttMessage();
        message.setPayload(jsonMapper.writeValueAsBytes(payload));
        message.setQos(getQos());
        message.setRetained(isRetained());
        try {
            mqttClient.publish(topic.asTopicString(), message);
        } catch (MqttException e) {
            monitor.handleFailedMessageDelivery(message, e);
        }
    }

    @Override
    public void connectionLost(Throwable throwable) {
        LOGGER.warn("Connection to MQTT broker lost.", throwable);
    }

    @Override
    public void messageArrived(String s, MqttMessage mqttMessage) {
        // We only publish messages but do not consume any. Thus, implementing this method is not needed.
        throw new UnsupportedOperationException();
    }

    @Override
    public void deliveryComplete(IMqttDeliveryToken token) {
        monitor.handleMessageDelivery(token);
    }

    @Override
    public void afterPropertiesSet() throws MqttException {
        logConfiguration();
        mqttClient = createMqttClient();
        mqttConnectOptions = createMqttConnectOptions();
        jsonMapper = createObjectMapper();
    }

    protected ObjectMapper createObjectMapper() {
        return new ObjectMapper();
    }

    protected MqttClient createMqttClient() throws MqttException {
        MqttClientPersistence persistence = createMqttClientPersistence();
        mqttClient = new MqttClient(getServerUri(), createClientId(), persistence);
        mqttClient.setCallback(this);
        return mqttClient;
    }


    protected MqttClientPersistence createMqttClientPersistence() {
        if (isFilePersistenceEnabled()) {
            return new MqttDefaultFilePersistence(getFilePersistenceDirectory());
        } else {
            return new MemoryPersistence();
        }
    }

    protected MqttConnectOptions createMqttConnectOptions() {
        MqttConnectOptions options = new MqttConnectOptions();
        options.setAutomaticReconnect(isReconnect());
        options.setCleanSession(isCleanSession());
        options.setConnectionTimeout(getConnectionTimeout());
        options.setKeepAliveInterval(getKeepAliveInterval());
        options.setMqttVersion(MqttConnectOptions.MQTT_VERSION_3_1_1);
        if (isBasicAuthentication()) {
            options.setUserName(getUsername());
            options.setPassword(getPassword().toCharArray());
        }
        options.setCleanSession(true);
        return options;
    }

    protected String createClientId() {
        if (getClientId() == null) {
            return String.join(".", getClientIdPrefix(), UUID.randomUUID().toString());
        } else {
            return String.join(".", getClientIdPrefix(), getClientId());
        }
    }

    @Override
    public void destroy() throws Exception {
        mqttClient.disconnect();
    }
}
