package org.n52.edis.pegelonlinekafkaconsumer.encode;

import org.n52.edis.pegelonlinekafkaconsumer.model.*;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;

@Component
public class MessageEncoder {

    public List<PegelonlineMqttMessage> encode(PegelonlineKafkaMessage payload) {
        List<PegelonlineMqttMessage> messages = new ArrayList<>();
        payload.getTimeseries().forEach(ts -> ts.getMeasurements().forEach(m -> {
            PegelonlineMqttMessage message = encode(payload, ts, m);
            messages.add(message);
        }));
        return messages;
    }

    public PegelonlineMqttMessage encode(PegelonlineKafkaMessage m, KafkaTimeseries t, Measurement msr) {
        Measurement measurement = new Measurement(msr.getTimestamp(), msr.getValue());
        MqttTimeseries timeseries = new MqttTimeseries(t.getUuid(), t.getShortname(), t.getLongname(), t.getUnit(),
                t.getEquidistance(), measurement);
        return new PegelonlineMqttMessage(m.getUuid(), m.getNumber(), m.getShortname(), m.getState(),
                m.getRegion(), m.getAgency(), m.getWater(), timeseries);
    }

}
