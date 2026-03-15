package no.hvl.dat110.messages;

import no.hvl.dat110.alotofnewstuff.MapMerger;

import java.util.Map;

public class SubscribeMsg extends Message {
    private String topic;
	// message sent from client to subscribe on a topic 

    public SubscribeMsg(String user, String topic) {
        super(MessageType.SUBSCRIBE, user);
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
