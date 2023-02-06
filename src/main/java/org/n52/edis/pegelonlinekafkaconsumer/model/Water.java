package org.n52.edis.pegelonlinekafkaconsumer.model;

public class Water {

    private String shortname;

    public Water() {
    }

    public Water(String shortname) {
        this.shortname = shortname;
    }

    public String getShortname() {
        return shortname;
    }

    public void setShortname(String shortname) {
        this.shortname = shortname;
    }

    @Override
    public String toString() {
        return "Water{" +
                "shortname='" + shortname + '\'' +
                '}';
    }
}
