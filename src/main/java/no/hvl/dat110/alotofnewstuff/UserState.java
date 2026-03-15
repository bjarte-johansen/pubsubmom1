package no.hvl.dat110.alotofnewstuff;

import no.hvl.dat110.broker.ClientSession;
import no.hvl.dat110.common.Logger;
import no.hvl.dat110.deprecated.PendingMessage;
import no.hvl.dat110.messages.Message;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;

public class UserState {
    // session is volatile to ensure visibility across threads, as multiple
    // threads may access and modify the session concurrently
    public volatile ClientSession session;

    // handle storing messages for users that are not connect
    public List<Message> mailbox = new ArrayList<>();


    /**
     * Set the client session for the user. This method is synchronized to ensure that only one thread can
     * @param session
     */
    public void setSession(ClientSession session) {
        this.session = session;
    }

    /**
     * Get the client session for the user. This method is synchronized to ensure that only one thread can access
     * @return
     */
    public ClientSession getSession() {
        return session;
    }

    /*
    mailbox is used to store messages for users that are not connected. When a user connects, the broker
    can check the mailbox and deliver any pending messages to the user. The pending map is used to track
    messages that have been sent to the user but have not yet been acknowledged. When a message is sent
    to the user, it is added to the pending map with a unique message ID. When an acknowledgment is received
    from the user, the corresponding entry in the pending map can be removed.
     */
    public void addToMailbox(Message msg) {
        Logger.log("Added message to mailbox for user " + msg.getUser() + ". Message added to mailbox.");
        mailbox.add(msg);
    }

    public List<Message> getMailboxMessages() {
        return mailbox;
    }

    public void addToMailbox(Collection<Message> messages) {
        mailbox.addAll(messages);
        mailbox.sort(Comparator.comparing(Message::getMessageId));
    }

    public void clearMailbox() {
        mailbox.clear();
    }
}
