package org.n52.edis.pegelonlinekafkaconsumer.model;

import java.util.List;

public class KafkaTimeseries extends Timeseries {

    private List<Measurement> measurements;

    public KafkaTimeseries() {
        super();
    }

    public KafkaTimeseries(String uuid, String shortname, String longname, String unit, float equidistance, List<Measurement> measurements) {
        super(uuid, shortname, longname, unit, equidistance);
        this.measurements = measurements;
    }

    public List<Measurement> getMeasurements() {
        return measurements;
    }

    public void setCurrentMeasurement(List<Measurement> measurements) {
        this.measurements = measurements;
    }

    @Override
    public String toString() {
        return "Timeseries{" +
                "shortname='" + getShortname() + '\'' +
                ", longname='" + getLongname() + '\'' +
                ", unit='" + getUnit() + '\'' +
                ", equidistance=" + getEquidistance() +
                ", measurements=" + measurements +
                '}';
    }
}
