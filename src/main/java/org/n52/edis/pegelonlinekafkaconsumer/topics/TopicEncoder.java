package org.n52.edis.pegelonlinekafkaconsumer.topics;

import org.n52.edis.pegelonlinekafkaconsumer.model.PegelonlineMessage;
import org.n52.edis.pegelonlinekafkaconsumer.model.Timeseries;

import java.util.List;
import java.util.stream.Collectors;

public class TopicEncoder {

    private String root;

    public TopicEncoder(String root) {
        this.root = root;
    }

    private final static String TOPIC_LEVEL_STATE_DEFAULT = "deutschland";
    private final static String TOPIC_LEVEL_REGION_DEFAULT = "NRW";

    public List<PegelonlineTopic> encode(PegelonlineMessage payload) {
        return payload.getTimeseries().stream().map(ts -> encode(payload, ts)).collect(Collectors.toList());
    }

    public PegelonlineTopic encode(PegelonlineMessage message, Timeseries timeseries) {
        PegelonlineTopic topic = new PegelonlineTopic();
        topic.setRoot(root);
        topic.setWater(message.getWater().getShortname());
        topic.setState(TOPIC_LEVEL_STATE_DEFAULT);
        topic.setRegion(TOPIC_LEVEL_REGION_DEFAULT);
        topic.setAgency(message.getAgency());
        topic.setUuid(message.getUuid());
        topic.setParameter(getParameterSubtopic(timeseries.getLongname()));
        return topic;
    }

    public String getParameterSubtopic(String longName) {
        return longName;
    }
}
