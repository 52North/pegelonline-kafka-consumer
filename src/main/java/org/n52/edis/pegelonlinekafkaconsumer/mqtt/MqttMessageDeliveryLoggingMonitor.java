package org.n52.edis.pegelonlinekafkaconsumer.mqtt;

import org.eclipse.paho.client.mqttv3.IMqttDeliveryToken;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.eclipse.paho.client.mqttv3.MqttMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;

@Component
@ConditionalOnProperty(prefix = "edis.mqtt", name = "service", havingValue = "logging", matchIfMissing = true)
public class MqttMessageDeliveryLoggingMonitor implements MqttMessageDeliveryMonitor {

    private final static Logger LOGGER = LoggerFactory.getLogger(MqttMessageDeliveryLoggingMonitor.class);

    @Override
    public void handleMessageDelivery(IMqttDeliveryToken token) {
        LOGGER.info("Successfully delivered message.");
        try {
            LOGGER.info("Message: {}", token.getMessage());
        } catch (MqttException e) {
            LOGGER.error("Error while fetching delivered message.", e);
        }
    }

    @Override
    public void handleFailedMessageDelivery(MqttMessage message, Exception e) {
        LOGGER.error("Error while publishing MQTT message. Cause: {}", e.getMessage());
        LOGGER.trace(String.format("Publishing MQTT message %s failed.", message), e);
    }
}
