package org.n52.edis.pegelonlinekafkaconsumer.model;

public class PegelonlineInternalTopic extends PegelonlineTopic {

    private String dan;

    public PegelonlineInternalTopic() {
        super();
    }

    public PegelonlineInternalTopic(String root, String water, String state, String region,
                                    String agency, String uuid, String parameter, String dan) {
        super(root, water, state, region, agency, uuid, parameter);
        this.dan = dan;
    }

    public String getDan() {
        return dan;
    }

    public void setDan(String dan) {
        this.dan = dan;
    }
}
