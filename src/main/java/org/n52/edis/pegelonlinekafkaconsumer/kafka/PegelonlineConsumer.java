package org.n52.edis.pegelonlinekafkaconsumer.kafka;

import com.fasterxml.jackson.core.JsonProcessingException;
import org.n52.edis.pegelonlinekafkaconsumer.encode.MessageEncoder;
import org.n52.edis.pegelonlinekafkaconsumer.encode.TopicEncoder;
import org.n52.edis.pegelonlinekafkaconsumer.model.PegelonlineKafkaMessage;
import org.n52.edis.pegelonlinekafkaconsumer.model.PegelonlineMqttMessage;
import org.n52.edis.pegelonlinekafkaconsumer.model.PegelonlineTopic;
import org.n52.edis.pegelonlinekafkaconsumer.mqtt.MqttPublisher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.listener.CommonErrorHandler;
import org.springframework.stereotype.Service;

@Service
public class PegelonlineConsumer implements InitializingBean {

    private static final Logger LOGGER = LoggerFactory.getLogger(PegelonlineConsumer.class);

    @Autowired(required = false)
    private MqttPublisher mqttPublisher;

    @Autowired
    private MessageEncoder messageEncoder;

    @Autowired
    private TopicEncoder topicEncoder;

    @Bean
    CommonErrorHandler errorHandler() {
        return new CustomErrorHandler();
    }

    @KafkaListener(id = "${edis.kafka.consumer.id}", topics = "${edis.kafka.consumer.topic}", groupId = "${edis.kafka.consumer.group}")
    public void consume(PegelonlineKafkaMessage message) {
        LOGGER.debug(String.format("Received message: -> %s", message));
        LOGGER.info(String.format("Received message for station: -> %s", message.getUuid()));
        message.getTimeseries().forEach(ts -> ts.getMeasurements().forEach(m -> {
            PegelonlineMqttMessage mqttMessage = messageEncoder.encode(message, ts, m);
            PegelonlineTopic topic = topicEncoder.encode(message, ts);
            try {
                if (mqttPublisher != null) {
                    LOGGER.info("Publish measurement for timeseries {} and timestamp {} via MQTT.",
                            mqttMessage.getTimeseries().getUuid(),
                            mqttMessage.getTimeseries().getMeasurement().getTimestamp());
                    if (mqttPublisher.isConnected()) {
                        mqttPublisher.publishMessage(mqttMessage, topic);
                    } else {
                        LOGGER.warn("MQTT client is not connected. Measurements for timeseries {} and timestamp {} could " +
                                        "not be published via MQTT.",
                                mqttMessage.getTimeseries().getUuid(),
                                mqttMessage.getTimeseries().getMeasurement().getTimestamp());
                    }
                } else {
                    LOGGER.info("Handled measurement in dev mode for timeseries {} and timestamp {}.",
                            mqttMessage.getTimeseries().getUuid(),
                            mqttMessage.getTimeseries().getMeasurement().getTimestamp());
                }
            } catch (JsonProcessingException e) {
                LOGGER.error("Error while publishing MQTT message", e);
            }
        }));
    }

    @Override
    public void afterPropertiesSet() throws Exception {
        mqttPublisher.connect();
    }
}
