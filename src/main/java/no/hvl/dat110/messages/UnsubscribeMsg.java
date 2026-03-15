package no.hvl.dat110.messages;

import no.hvl.dat110.alotofnewstuff.MapMerger;

import java.util.Map;

public class UnsubscribeMsg extends Message {
    private String topic;
    // message sent from client to subscribe on a topic

	// message sent from client to unsubscribe on a topic 

    public UnsubscribeMsg(String user, String topic) {
        super(MessageType.UNSUBSCRIBE, user);
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


/*
    @Override
    public String toString() {
        return getClass().getSimpleName() + MapMerger.merge(Map.of("topic", getTopic()), super.getAttributeMap())String.format("UnsubscribeMsg [topic=\"%s\"] %s", getTopic(), super.toString());
    }
*/
}
