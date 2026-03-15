package no.hvl.dat110.messages;

import no.hvl.dat110.alotofnewstuff.MapMerger;
import no.hvl.dat110.common.TODO;

import java.util.Map;

public class PublishMsg extends Message {
    private String topic;
    private String message;
	
	// message sent from client to create publish a message on a topic 

	public PublishMsg(String user, String topic, String message) {
        super(MessageType.PUBLISH, user);
        this.topic = topic;
        this.message = message;
    }

    public String getTopic() {
        return topic;
    }
    public void setTopic(String topic) {
        this.topic = topic;
    }

    public String getMessage() {
        return message;
    }
    public void setMessage(String message) {
        this.message = message;
    }

    @Override
    protected Map<String, Object> getAttributeMap() {
        return MapMerger.merge(Map.of("topic", getTopic()), super.getAttributeMap());
    }
}
