package no.hvl.dat110.broker;

import no.hvl.dat110.alotofnewstuff.ReliableSender;
import no.hvl.dat110.common.LoggerScope;
import no.hvl.dat110.deprecated.PendingMessage;
import no.hvl.dat110.common.Logger;
import no.hvl.dat110.messages.AckMsg;
import no.hvl.dat110.messages.Message;
import no.hvl.dat110.messages.MessageUtils;
import no.hvl.dat110.messagetransport.Connection;

import java.io.IOException;

public class ClientSession extends Thread{

	private final String user;
	
	// underlying message transport connection
	private final Connection connection;
    private final Dispatcher dispatcher;
    public final ReliableSender reliableSender;


    /**
     * Constructor for ClientSession, initializes the user, connection, dispatcher, and reliableSender
     * @param user the username associated with this client session
     * @param connection the underlying message transport connection for this client session
     * @param dispatcher the dispatcher to handle incoming messages for this client session
     */

	public ClientSession(String user, Connection connection, Dispatcher dispatcher) {
		this.user = user;
		this.connection = connection;
        this.dispatcher = dispatcher;
        this.reliableSender = new ReliableSender(this);
	}

    /**
     * Disconnect the client session by closing the underlying connection
     */

	public void disconnect() {
		if (connection != null && !connection.isClosed()) {
            connection.close();
		}
	}


    /**
     * Get the username associated with this client session
     * @return the username
     */
/*
	public String getUser() {
		return user;
	}
 */


    /**
     * Send message directly without reliability, used for control messages like ACK, CONNECT, DISCONNECT, etc.
     * @param message
     */

	public void send(Message message) {
        MessageUtils.send(connection, message);
	}


    /**
     * Send message reliably by using the reliableSender, which will handle retries and ACKs
     * @param message
     */

    public void sendReliable(Message message) {
        //Logger.log("TRY sendReliable: " + message);
        reliableSender.send(message);
        //Logger.log("OK sendReliable: " + message);
    }


    /**
     * Handle received message, if ACK then handle with reliableSender, otherwise dispatch to broker
     * @param message
     */

    public void onReceive(Message message) {
        Logger.log(message.toString());

        if(message instanceof AckMsg) {
            Logger.log("Ack reliableSender " + message.toString());
            reliableSender.ack(message.getMessageId());
            return;
        }

        try(LoggerScope tmp = Logger.scope("Dispatching")) {
            dispatcher.dispatch(this, message);
        }
    }


    /**
     * Run method for the client session thread, continuously receive messages and handle them until connection is closed
     */

    @Override
    public void run() {
        try {
            while (connection != null && !connection.isClosed()) {
                // when block until receive message, trigger event handler
                Message msg = MessageUtils.receive(connection);
                onReceive(msg);
            }
        }catch (IOException e) {
            Logger.log("Connection lost, " + e.getMessage());
        }finally {
            disconnect();
        }
    }
}
