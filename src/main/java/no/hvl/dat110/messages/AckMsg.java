package no.hvl.dat110.messages;

import no.hvl.dat110.alotofnewstuff.MapMerger;

import java.util.Map;

public class AckMsg extends Message {
    // message sent from client to create publish a message on a topic

    public AckMsg(String user, Integer messageId) {
        super(MessageType.ACK, user);
        this.setMessageId(messageId);
    }
}
