package no.hvl.dat110.messages;

import no.hvl.dat110.alotofnewstuff.MapAsAttributesString;
import no.hvl.dat110.alotofnewstuff.MapUtils;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

public abstract class Message {
    protected static final Integer defaultId = Integer.valueOf(-1);

    private Integer id = defaultId;
	private final MessageType type;
	private final String user;

	public Message(MessageType type, String user) {
        this.id = null;
		this.type = type;
		this.user = user;
	}

    public void setMessageId(Integer id) {this.id = id; }
    public Integer getMessageId() {return id; }

	public MessageType getType() { return this.type; }
    public String getUser() {return user; }

    protected Map<String, Object> getAttributeMap() {
        return MapUtils.unsafeOf("id", getMessageId(), "type", getType(), "user", getUser());
    }

    public String toString(){
        return getClass().getSimpleName() + " {" + MapAsAttributesString.toString(getAttributeMap()) + "}";
    }
}
