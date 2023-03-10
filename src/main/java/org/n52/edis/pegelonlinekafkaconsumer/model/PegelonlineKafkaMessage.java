package org.n52.edis.pegelonlinekafkaconsumer.model;

import java.util.List;

public class PegelonlineKafkaMessage extends PegelonlineMessage{

    private List<KafkaTimeseries> timeseries;

    public PegelonlineKafkaMessage() {
        super();
    }

    public PegelonlineKafkaMessage(String uuid, String number, String shortname, String state, String region,
                                   String agency, Water water, List<KafkaTimeseries> timeseries) {
        super(uuid, number, shortname, state, region, agency, water);
        this.timeseries = timeseries;
    }

    public List<KafkaTimeseries> getTimeseries() {
        return timeseries;
    }

    public void setTimeseries(List<KafkaTimeseries> timeseries) {
        this.timeseries = timeseries;
    }

    @Override
    public String toString() {
        return "PegelonlineMessage{" +
                "uuid='" + getUuid() + '\'' +
                ", number='" + getNumber() + '\'' +
                ", shortname='" + getShortname() + '\'' +
                ", state='" + getState() + '\'' +
                ", region='" + getRegion() + '\'' +
                ", agency='" + getAgency() + '\'' +
                ", water=" + getWater() +
                ", timeseries=" + timeseries +
                '}';
    }
}
