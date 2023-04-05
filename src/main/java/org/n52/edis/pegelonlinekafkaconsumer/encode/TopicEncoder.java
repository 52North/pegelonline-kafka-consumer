package org.n52.edis.pegelonlinekafkaconsumer.encode;

import org.n52.edis.pegelonlinekafkaconsumer.model.*;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.stream.Collectors;

@Component
public class TopicEncoder {

    private static final String WHITESPACE_TOKEN = "\\s+";
    private static final String UNDERSCORE_TOKEN = "_";

    @Value("${edis.mqtt.base-topic}")
    private String baseTopic;

    public List<PegelonlineTopic> encode(PegelonlineKafkaMessage payload) {
        return payload.getTimeseries().stream().map(ts -> encode(payload, ts)).collect(Collectors.toList());
    }

    public PegelonlineTopic encode(PegelonlineKafkaMessage message, KafkaTimeseries timeseries) {
        PegelonlineTopic topic = new PegelonlineTopic();
        topic.setRoot(baseTopic);
        topic.setWater(normalizeTopicLevel(message.getWater().getShortname()));
        topic.setState(normalizeTopicLevel(message.getState()));
        topic.setRegion(normalizeTopicLevel(message.getRegion()));
        topic.setAgency(normalizeTopicLevel(message.getAgency()));
        topic.setUuid(message.getUuid());
        topic.setParameter(normalizeTopicLevel(timeseries.getShortname()));
        return topic;
    }

    private String normalizeTopicLevel(String level) {
        level = level.replaceAll(WHITESPACE_TOKEN, UNDERSCORE_TOKEN);
        level = level.toLowerCase();
        return level;
    }
}
