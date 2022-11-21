package org.n52.edis.pegelonlinekafkaconsumer.model;

public class Timeseries {
    private String shortname;

    private String longname;

    private String unit;

    private float equidistance;

    private Measurement currentMeasurement;


    public Timeseries() {
    }

    public Timeseries(String shortname, String longname, String unit, float equidistance, Measurement currentMeasurement) {
        this.shortname = shortname;
        this.longname = longname;
        this.unit = unit;
        this.equidistance = equidistance;
        this.currentMeasurement = currentMeasurement;
    }

    public String getShortname() {
        return shortname;
    }

    public void setShortname(String shortname) {
        this.shortname = shortname;
    }

    public String getLongname() {
        return longname;
    }

    public void setLongname(String longname) {
        this.longname = longname;
    }

    public String getUnit() {
        return unit;
    }

    public void setUnit(String unit) {
        this.unit = unit;
    }

    public float getEquidistance() {
        return equidistance;
    }

    public void setEquidistance(float equidistance) {
        this.equidistance = equidistance;
    }

    public Measurement getCurrentMeasurement() {
        return currentMeasurement;
    }

    public void setCurrentMeasurement(Measurement currentMeasurement) {
        this.currentMeasurement = currentMeasurement;
    }

    @Override
    public String toString() {
        return "Timeseries{" +
                "shortname='" + shortname + '\'' +
                ", longname='" + longname + '\'' +
                ", unit='" + unit + '\'' +
                ", equidistance=" + equidistance +
                ", currentMeasurement=" + currentMeasurement +
                '}';
    }
}
