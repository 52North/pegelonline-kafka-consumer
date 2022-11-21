package org.n52.edis.pegelonlinekafkaconsumer.model;

public class Measurement {
    private String timestamp;

    private double value;

    private double trend;

    private String stateMnwMhw;

    private String stateNswHsw;

    public Measurement() {

    }

    public Measurement(String timestamp, double value, double trend, String stateMnwMhw, String stateNswHsw) {
        this.timestamp = timestamp;
        this.value = value;
        this.trend = trend;
        this.stateMnwMhw = stateMnwMhw;
        this.stateNswHsw = stateNswHsw;
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

    public double getTrend() {
        return trend;
    }

    public void setTrend(double trend) {
        this.trend = trend;
    }

    public String getStateMnwMhw() {
        return stateMnwMhw;
    }

    public void setStateMnwMhw(String stateMnwMhw) {
        this.stateMnwMhw = stateMnwMhw;
    }

    public String getStateNswHsw() {
        return stateNswHsw;
    }

    public void setStateNswHsw(String stateNswHsw) {
        this.stateNswHsw = stateNswHsw;
    }

    @Override
    public String toString() {
        return "Measurement{" +
                "timestamp='" + timestamp + '\'' +
                ", value=" + value +
                ", trend=" + trend +
                ", stateMnwMhw='" + stateMnwMhw + '\'' +
                ", stateNswHsw='" + stateNswHsw + '\'' +
                '}';
    }
}
