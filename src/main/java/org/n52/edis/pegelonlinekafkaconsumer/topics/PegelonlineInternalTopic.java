package org.n52.edis.pegelonlinekafkaconsumer.topics;

public class PegelonlineInternalTopic extends PegelonlineTopic {

    private String dan;

    public PegelonlineInternalTopic() {
        super();
    }

    public PegelonlineInternalTopic(String root, String project, String water, String state, String region,
                                    String agency, String uuid, String parameter, String dan) {
        super(root, project, water, state, region, agency, uuid, parameter);
        this.dan = dan;
    }
}
