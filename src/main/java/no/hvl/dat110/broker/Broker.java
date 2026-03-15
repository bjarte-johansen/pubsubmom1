package no.hvl.dat110.broker;

import no.hvl.dat110.common.Logger;
import no.hvl.dat110.common.Stopable;
import no.hvl.dat110.messages.ConnectMsg;
import no.hvl.dat110.messages.Message;
import no.hvl.dat110.messages.MessageType;
import no.hvl.dat110.messages.MessageUtils;
import no.hvl.dat110.messagetransport.Connection;
import no.hvl.dat110.messagetransport.MessagingServer;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;

public class Broker extends Stopable {
    static private AtomicInteger nextId = new AtomicInteger(1);
    public static int CREATE_UNIQUE_MESSAGE_ID() { return nextId.getAndIncrement(); }

	private boolean stopable = false;
	private int numConnectionsRemaining = 0;
	
	private MessagingServer server;
	private Dispatcher dispatcher;

    public MessagingServer getMessagingServer() { return server; }

		
	public Broker (Dispatcher dispatcher,int port) {
		super("Broker");
		server = new MessagingServer(port);
		this.dispatcher = dispatcher;
	}
	
	public void setMaxAccept(int n) {
		this.stopable = true;
		this.numConnectionsRemaining = n;
	}
	
	@Override
	public void doProcess() {

        Logger.log("Broker accept [" + numConnectionsRemaining + "]");

        Connection connection = server.accept();

        Logger.log("!" + numConnectionsRemaining);

        try {
            waitConnect(connection);
        }catch(IOException ex) {
            System.out.println("Broker: " + ex.getMessage());
            ex.printStackTrace();
        }

        if (stopable) {
            numConnectionsRemaining--;

            if (numConnectionsRemaining < 1) {
                super.doStop();
            }
        }
	}

	private void waitConnect(Connection connection) throws IOException {
		Message msg = MessageUtils.receive(connection);

		if (msg.getType() == MessageType.CONNECT) {
			ConnectMsg cmsg = (ConnectMsg) msg;
			dispatcher.onConnect(cmsg, connection, dispatcher);
			
		} else {
			System.out.println("Protocol error: first message should be connect");
		}
	}
	
}
