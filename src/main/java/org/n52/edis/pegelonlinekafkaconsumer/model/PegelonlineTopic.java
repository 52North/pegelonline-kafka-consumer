package org.n52.edis.pegelonlinekafkaconsumer.model;

public class PegelonlineTopic {

    private static final String TOPIC_DELIMITER = "/";

    private String root;

    private String project;

    private String water;

    private String state;

    private String region;

    private String agency;

    private String uuid;

    private String parameter;

    public PegelonlineTopic() {
    }

    public PegelonlineTopic(String root, String water, String state, String region, String agency, String uuid, String parameter) {
        this.root = root;
        this.water = water;
        this.state = state;
        this.region = region;
        this.agency = agency;
        this.uuid = uuid;
        this.parameter = parameter;
    }

    public String getRoot() {
        return root;
    }

    public void setRoot(String root) {
        this.root = root;
    }

    public String getWater() {
        return water;
    }

    public void setWater(String water) {
        this.water = water;
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

    public String getUuid() {
        return uuid;
    }

    public void setUuid(String uuid) {
        this.uuid = uuid;
    }

    public String getParameter() {
        return parameter;
    }

    public void setParameter(String parameter) {
        this.parameter = parameter;
    }

    public String asTopicString() {
        return String.join(TOPIC_DELIMITER, getRoot(), getWater(), getState(), getRegion(), getAgency(), getUuid(),
                getParameter());
    }
}
