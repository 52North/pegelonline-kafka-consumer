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

import java.util.UUID;

public class MqttPublisher extends AbstractMqttPublisher implements MqttCallback, InitializingBean, DisposableBean {

    private static final Logger LOGGER = LoggerFactory.getLogger(MqttPublisher.class);

    private ObjectMapper jsonMapper;

    private MqttClient mqttClient;

    private MqttMessageDeliveryMonitor monitor;

    public MqttPublisher() {

    }

    public MqttPublisher(MqttMessageDeliveryMonitor monitor) {
        this.monitor = monitor;
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
    }

    @Override
    public void deliveryComplete(IMqttDeliveryToken token) {
        monitor.handleMessageDelivery(token);
    }

    @Override
    public void afterPropertiesSet() throws Exception {
        logConfiguration();
        mqttClient = createMqttClient();
        jsonMapper = createObjectMapper();
    }

    private ObjectMapper createObjectMapper() {
        return new ObjectMapper();
    }

    private MqttClient createMqttClient() throws MqttException {
        MqttClientPersistence persistence = createMqttClientPersistence();
        mqttClient = new MqttClient(getServerUri(), createClientId(), persistence);
        mqttClient.setCallback(this);
        mqttClient.connect(createMqttConnectOptions());
        return mqttClient;
    }


    private MqttClientPersistence createMqttClientPersistence() {
        if (isFilePersistenceEnabled()) {
            return new MqttDefaultFilePersistence(getFilePersistenceDirectory());
        } else {
            return new MemoryPersistence();
        }
    }

    private MqttConnectOptions createMqttConnectOptions() {
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
        return options;
    }

    private String createClientId() {
        return String.join(".", getClientIdPrefix(), UUID.randomUUID().toString());
    }


    @Override
    public void destroy() throws Exception {
        mqttClient.disconnect();
    }
}
