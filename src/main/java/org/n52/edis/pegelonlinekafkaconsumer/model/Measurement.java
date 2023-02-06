package org.n52.edis.pegelonlinekafkaconsumer.model;

public class Measurement {
    private String timestamp;

    private double value;

    public Measurement() {

    }

    public Measurement(String timestamp, double value) {
        this.timestamp = timestamp;
        this.value = value;
    }

    public String getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(String timestamp) {
        this.timestamp = timestamp;
    }

    public double getValue() {
        return value;
    }

    public void setValue(double value) {
        this.value = value;
    }

    @Override
    public String toString() {
        return "Measurement{" +
                "timestamp='" + timestamp + '\'' +
                ", value=" + value +
                '}';
    }
}
