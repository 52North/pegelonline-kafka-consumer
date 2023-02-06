package org.n52.edis.pegelonlinekafkaconsumer.model;

public class Timeseries {

    private String uuid;

    private String shortname;

    private String longname;

    private String unit;

    private float equidistance;

    public Timeseries() {
    }

    public Timeseries(String uuid, String shortname, String longname, String unit, float equidistance) {
        this.uuid = uuid;
        this.shortname = shortname;
        this.longname = longname;
        this.unit = unit;
        this.equidistance = equidistance;
    }

    public String getUuid() {
        return uuid;
    }

    public void setUuid(String uuid) {
        this.uuid = uuid;
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

    @Override
    public String toString() {
        return "Timeseries{" +
                "uuid='" + uuid + '\'' +
                ", shortname='" + shortname + '\'' +
                ", longname='" + longname + '\'' +
                ", unit='" + unit + '\'' +
                ", equidistance=" + equidistance +
                '}';
    }
}
