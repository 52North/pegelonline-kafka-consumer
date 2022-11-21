package org.n52.edis.pegelonlinekafkaconsumer.model;

public class Water {

    private String shortname;

    private String longname;

    public Water() {
    }

    public Water(String shortname, String longname) {
        this.shortname = shortname;
        this.longname = longname;
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

    @Override
    public String toString() {
        return "Water{" +
                "shortname='" + shortname + '\'' +
                ", longname='" + longname + '\'' +
                '}';
    }
}
