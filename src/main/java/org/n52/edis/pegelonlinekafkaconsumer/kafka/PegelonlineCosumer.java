package org.n52.edis.pegelonlinekafkaconsumer.kafka;

import org.n52.edis.pegelonlinekafkaconsumer.model.PegelonlineMessage;
import org.n52.edis.pegelonlinekafkaconsumer.mqtt.MqttPublisher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

import java.io.IOException;

@Service
public class PegelonlineCosumer {

    private static final Logger LOGGER = LoggerFactory.getLogger(PegelonlineCosumer.class);

    @Autowired
    private MqttPublisher mqttPublisher;

    @KafkaListener(id = "cosumer1", topics = "${edis.kafka.consumer.topic}", groupId = "org.n52.edis.pegelonline")
    public void consume(PegelonlineMessage message) throws IOException {
        LOGGER.debug(String.format("Consumed message: -> %s", message));
        LOGGER.info(String.format("Consumed message for station: -> %s", message.getShortname()));
        mqttPublisher.publishMessage(message);
    }
}
