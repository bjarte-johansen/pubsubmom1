package no.hvl.dat110.alotofnewstuff;

import no.hvl.dat110.broker.Broker;
import no.hvl.dat110.broker.ClientSession;
import no.hvl.dat110.deprecated.PendingMessage;
import no.hvl.dat110.messages.Message;

import java.util.ArrayList;
import java.util.Collection;
import java.util.concurrent.ConcurrentHashMap;

public class ReliableSender {
    ClientSession session;
    ConcurrentHashMap<Integer, PendingMessage> pending;

    public ReliableSender(ClientSession session){
        this.session = session;
        this.pending = new ConcurrentHashMap<>();
    }

    public void send(Message msg){
        int id = Broker.CREATE_UNIQUE_MESSAGE_ID();
        PendingMessage pendingMessage = new PendingMessage(msg, id);

        msg.setMessageId(id);
        pending.put(id, pendingMessage);

        session.send(msg);
    }

    public void ack(int id){
        pending.remove(id);
    }

    // chatgpt told us "drain" vs common wording to remove and collect all items
    // in networking apis, queues and similar. we bought it.
    public Collection<PendingMessage> drainPending(){
        var result = new ArrayList<PendingMessage>( pending.values() );
        pending.clear();
        return result;
    }
}
