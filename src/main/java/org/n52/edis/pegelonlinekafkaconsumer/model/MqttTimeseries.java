package org.n52.edis.pegelonlinekafkaconsumer.model;

public class MqttTimeseries extends Timeseries {

    private Measurement measurement;


    public MqttTimeseries() {
        super();
    }

    public MqttTimeseries(String uuid, String shortname, String longname, String unit, float equidistance, Measurement measurement) {
        super(uuid, shortname, longname, unit, equidistance);
        this.measurement = measurement;
    }

    public Measurement getMeasurement() {
        return measurement;
    }

    public void setMeasurement(Measurement measurement) {
        this.measurement = measurement;
    }

    @Override
    public String toString() {
        return "Timeseries{" +
                "shortname='" + getShortname() + '\'' +
                ", longname='" + getLongname() + '\'' +
                ", unit='" + getUnit() + '\'' +
                ", equidistance=" + getEquidistance() +
                ", currentMeasurement=" + measurement +
                '}';
    }
}
