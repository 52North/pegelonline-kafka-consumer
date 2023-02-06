package org.n52.edis.pegelonlinekafkaconsumer.model;

public class PegelonlineMessage {
    private String uuid;

    private String number;

    private String shortname;

    private String state;

    private String region;

    private String agency;

    private Water water;

    public PegelonlineMessage() {

    }

    public PegelonlineMessage(String uuid, String number, String shortname, String state, String region, String agency, Water water) {
        this.uuid = uuid;
        this.number = number;
        this.shortname = shortname;
        this.state = state;
        this.region = region;
        this.agency = agency;
        this.water = water;
    }

    public String getUuid() {
        return uuid;
    }

    public void setUuid(String uuid) {
        this.uuid = uuid;
    }

    public String getNumber() {
        return number;
    }

    public void setNumber(String number) {
        this.number = number;
    }

    public String getShortname() {
        return shortname;
    }

    public void setShortname(String shortname) {
        this.shortname = shortname;
    }

    public String getState() {
        return state;
    }

    public void setState(String state) {
        this.state = state;
    }

    public String getRegion() {
        return region;
    }

    public void setRegion(String region) {
        this.region = region;
    }

    public String getAgency() {
        return agency;
    }

    public void setAgency(String agency) {
        this.agency = agency;
    }

    public Water getWater() {
        return water;
    }

    public void setWater(Water water) {
        this.water = water;
    }

    @Override
    public String toString() {
        return "PegelonlineMessage{" +
                "uuid='" + uuid + '\'' +
                ", number='" + number + '\'' +
                ", shortname='" + shortname + '\'' +
                ", state='" + state + '\'' +
                ", region='" + region + '\'' +
                ", agency='" + agency + '\'' +
                ", water=" + water +
                '}';
    }
}
