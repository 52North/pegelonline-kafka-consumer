package org.n52.edis.pegelonlinekafkaconsumer.model;

import java.util.List;

public class PegelonlineMessage {
    private String uuid;

    private String number;

    private String shortname;

    private String longname;

    private float km;

    private String agency;

    private double longitude;

    private double latitude;

    private Water water;

    private List<Timeseries> timeseries;

    private String name;


    public PegelonlineMessage() {
    }

    public PegelonlineMessage(String uuid, String number, String shortname, String longname, float km, String agency,
                              double longitude, double latitude, Water water, List<Timeseries> timeseries, String name) {
        this.uuid = uuid;
        this.number = number;
        this.shortname = shortname;
        this.longname = longname;
        this.km = km;
        this.agency = agency;
        this.longitude = longitude;
        this.latitude = latitude;
        this.water = water;
        this.timeseries = timeseries;
        this.name = name;
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

    public String getLongname() {
        return longname;
    }

    public void setLongname(String longname) {
        this.longname = longname;
    }

    public float getKm() {
        return km;
    }

    public void setKm(float km) {
        this.km = km;
    }

    public String getAgency() {
        return agency;
    }

    public void setAgency(String agency) {
        this.agency = agency;
    }

    public double getLongitude() {
        return longitude;
    }

    public void setLongitude(double longitude) {
        this.longitude = longitude;
    }

    public double getLatitude() {
        return latitude;
    }

    public void setLatitude(double latitude) {
        this.latitude = latitude;
    }

    public Water getWater() {
        return water;
    }

    public void setWater(Water water) {
        this.water = water;
    }

    public List<Timeseries> getTimeseries() {
        return timeseries;
    }

    public void setTimeseries(List<Timeseries> timeseries) {
        this.timeseries = timeseries;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    @Override
    public String toString() {
        return "PegelonlineMessage{" +
                "uuid='" + uuid + '\'' +
                ", number='" + number + '\'' +
                ", shortname='" + shortname + '\'' +
                ", longname='" + longname + '\'' +
                ", km=" + km +
                ", agency='" + agency + '\'' +
                ", longitude=" + longitude +
                ", latitude=" + latitude +
                ", water=" + water +
                ", timeseries=" + timeseries +
                ", name='" + name + '\'' +
                '}';
    }
}
