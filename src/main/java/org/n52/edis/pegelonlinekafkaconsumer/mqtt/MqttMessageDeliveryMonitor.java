package org.n52.edis.pegelonlinekafkaconsumer.mqtt;

import org.eclipse.paho.client.mqttv3.IMqttDeliveryToken;
import org.eclipse.paho.client.mqttv3.MqttMessage;

public interface MqttMessageDeliveryMonitor {
    void handleMessageDelivery(IMqttDeliveryToken token);

    void handleFailedMessageDelivery(MqttMessage message, Exception e);
}
