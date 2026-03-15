package no.hvl.dat110.deprecated;

import no.hvl.dat110.messages.Message;

public class PendingMessage {
    public Message message;
    public int messageId;

    public PendingMessage(){
    }
    public PendingMessage(Message message, int messageId) {
        this.message = message;
        this.messageId = messageId;
    }

    public Message getMessage() {return message;}
    public void setMessage(Message message) {this.message = message;}

    public int getMessageId() {return messageId;}
    public void setMessageId(int messageId) {this.messageId = messageId;}
}
