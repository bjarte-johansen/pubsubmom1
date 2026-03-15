package no.hvl.dat110.messages;

import no.hvl.dat110.alotofnewstuff.MapMerger;

import java.util.Map;

public class DeleteTopicMsg extends Message {
    private String topic;
	// message sent from client to create topic on the broker

    public DeleteTopicMsg(String user, String topic) {
        super(MessageType.DELETETOPIC, user);
        this.topic = topic;
    }

    public String getTopic() {
        return topic;
    }
    public void setTopic(String topic) {
        this.topic = topic;
    }

    @Override
    protected Map<String, Object> getAttributeMap() {
        return MapMerger.merge(Map.of("topic", getTopic()), super.getAttributeMap());
    }
}
