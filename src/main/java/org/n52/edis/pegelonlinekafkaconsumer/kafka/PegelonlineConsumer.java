package org.n52.edis.pegelonlinekafkaconsumer.kafka;

import com.fasterxml.jackson.core.JacksonException;
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
import org.springframework.kafka.listener.ConsumerRecordRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.support.serializer.DeserializationException;
import org.springframework.stereotype.Service;
import org.springframework.util.backoff.FixedBackOff;

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
        ConsumerRecordRecoverer recoverer = (record, exception) -> {
            DeserializationException deserEx = findDeserializationException(exception);
            if (deserEx != null) {
                LOGGER.error("Can not deserialize message '{}' on topic '{}'. Skipping record.",
                        new String(deserEx.getData()), record.topic());
                LOGGER.debug("Deserialization error.", deserEx);
            } else {
                LOGGER.error("Unrecoverable error while consuming message on topic '{}'. Skipping record. Cause: {}",
                        record.topic(), exception.getMessage());
                LOGGER.debug("Consuming message error.", exception);
            }
        };
        return new DefaultErrorHandler(recoverer, new FixedBackOff(1000L, 2));
    }

    private static DeserializationException findDeserializationException(Exception exception) {
        Throwable cause = exception;
        while (cause != null) {
            if (cause instanceof DeserializationException deserEx) {
                return deserEx;
            }
            cause = cause.getCause();
        }
        return null;
    }

    @KafkaListener(id = "${edis.kafka.consumer.id}", topics = "${edis.kafka.consumer.topic}", groupId = "${edis.kafka.consumer.group}")
    public void consume(PegelonlineKafkaMessage message) {
        LOGGER.debug("Received message: -> {}", message);
        LOGGER.info("Received message for station: -> {}", message.getUuid());
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
            } catch (JacksonException e) {
                LOGGER.error("Error while publishing MQTT message", e);
            }
        }));
    }

    @Override
    public void afterPropertiesSet() throws Exception {
        mqttPublisher.connect();
    }
}
