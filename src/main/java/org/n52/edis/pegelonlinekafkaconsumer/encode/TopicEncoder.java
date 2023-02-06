package org.n52.edis.pegelonlinekafkaconsumer.encode;

import org.n52.edis.pegelonlinekafkaconsumer.model.*;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.stream.Collectors;

@Component
public class TopicEncoder {

    @Value("${edis.mqtt.base-topic}")
    private String baseTopic;

    private final static String TOPIC_LEVEL_STATE_DEFAULT = "deutschland";

    public List<PegelonlineTopic> encode(PegelonlineKafkaMessage payload) {
        return payload.getTimeseries().stream().map(ts -> encode(payload, ts)).collect(Collectors.toList());
    }

    public PegelonlineTopic encode(PegelonlineKafkaMessage message, KafkaTimeseries timeseries) {
        PegelonlineTopic topic = new PegelonlineTopic();
        topic.setRoot(baseTopic);
        topic.setWater(message.getWater().getShortname());
        topic.setState(message.getState());
        topic.setRegion(message.getRegion());
        topic.setAgency(message.getAgency());
        topic.setUuid(message.getUuid());
        topic.setParameter(timeseries.getLongname());
        return topic;
    }
}
