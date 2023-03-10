package org.n52.edis.pegelonlinekafkaconsumer.model;

public class PegelonlineMqttMessage extends PegelonlineMessage {

    private MqttTimeseries timeseries;

    public PegelonlineMqttMessage() {
        super();

    }

    public PegelonlineMqttMessage(String uuid, String number, String shortname, String state, String region,
                                  String agency, Water water, MqttTimeseries timeseries) {
        super(uuid, number, shortname, state, region, agency, water);
        this.timeseries = timeseries;
    }

    public MqttTimeseries getTimeseries() {
        return timeseries;
    }

    public void setTimeseries(MqttTimeseries timeseries) {
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
