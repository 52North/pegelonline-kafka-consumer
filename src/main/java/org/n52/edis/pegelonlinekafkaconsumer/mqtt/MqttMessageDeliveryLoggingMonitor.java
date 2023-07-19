package org.n52.edis.pegelonlinekafkaconsumer.mqtt;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.eclipse.paho.client.mqttv3.IMqttDeliveryToken;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.eclipse.paho.client.mqttv3.MqttMessage;
import org.n52.edis.pegelonlinekafkaconsumer.model.PegelonlineMqttMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;

import java.io.IOException;

@Component
@ConditionalOnProperty(prefix = "edis.mqtt", name = "service", havingValue = "logging", matchIfMissing = true)
public class MqttMessageDeliveryLoggingMonitor implements MqttMessageDeliveryMonitor, InitializingBean {

    private static final Logger LOGGER = LoggerFactory.getLogger(MqttMessageDeliveryLoggingMonitor.class);

    private ObjectMapper jsonMapper;

    @Override
    public void handleMessageDelivery(IMqttDeliveryToken token) {
        try {
            LOGGER.info("Successfully published measurement on topic {}.", token.getTopics()[0]);
            if(token.getMessage() != null) {
                PegelonlineMqttMessage message = jsonMapper.readValue(token.getMessage().getPayload(), PegelonlineMqttMessage.class);
                LOGGER.debug("Published message: {}", message);
            }
        } catch (MqttException | IOException e) {
            LOGGER.error("Error while monitoring measurement delivery on topic {}. Cause: {}",
                    token.getTopics()[0], e.getMessage());
            LOGGER.trace("Measurement delivery monitoring failed.", e);
        }
    }

    @Override
    public void handleFailedMessageDelivery(MqttMessage message, Exception e) {
        try {
            PegelonlineMqttMessage m = jsonMapper.readValue(message.getPayload(), PegelonlineMqttMessage.class);
            LOGGER.error("Error while publishing MQTT message for timeseries {} and timestamp {}. Cause: {}",
                    m.getTimeseries().getUuid(),
                    m.getTimeseries().getMeasurement().getTimestamp(),
                    e.getMessage());
            LOGGER.trace(String.format("Publishing MQTT message %s failed.", message), e);
        } catch (IOException ex) {
            LOGGER.error("Error while monitoring measurement delivery fail. Cause: {}", e.getMessage());
            LOGGER.trace("Failed measurement delivery monitoring failed.", e);
        }

    }

    @Override
    public void afterPropertiesSet() throws Exception {
        jsonMapper = createObjectMapper();
    }

    private ObjectMapper createObjectMapper() {
        return new ObjectMapper();
    }
}
