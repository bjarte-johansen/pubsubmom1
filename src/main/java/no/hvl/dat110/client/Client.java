package no.hvl.dat110.client;

import no.hvl.dat110.common.Logger;
import no.hvl.dat110.messages.*;
import no.hvl.dat110.messagetransport.Connection;
import no.hvl.dat110.messagetransport.MessagingClient;

import java.io.IOException;

public class Client extends Thread {

	private MessagingClient client;
	private Connection connection;
	private String user;

	public Client(String user, String server, int port) {
		client = new MessagingClient(server, port);
		this.user = user;
	}

	private void send(Message msg) {
        if(connection == null) {
            Logger.log("Connection is null");
            return;
        }

		connection.send(MessageUtils.toTransportMessage(msg));
	}

	public Message receive() throws IOException {
        Message msg =  MessageUtils.fromTransportMessage(connection.receive());

        Logger.log("Received " + msg.getType() + " {id: " + msg.getMessageId() + "}");

        // handle message type by sending ack
        if(msg.getType() == MessageType.PUBLISH) {
            // send ACK for received message
            AckMsg ackMsg = new AckMsg(user, msg.getMessageId());
            send(ackMsg);

            // log
            Logger.log("Sent ACK for message ID [" + msg.getMessageId() + "]");
        }

        return msg;
	}

	public boolean connect() {
		boolean connected = false;

        Logger.log("Client attempting to connect ...");

		connection = client.connect();

		if (connection != null) {
            ConnectMsg msg = new ConnectMsg(user);
			send(msg);

			connected = true;
		}

		return connected;
	}

	public void disconnect() {
		DisconnectMsg msg = new DisconnectMsg(user);
		send(msg);
		connection.close();
	}

	public void subscribe(String topic) {
		SubscribeMsg msg = new SubscribeMsg(user, topic);
		send(msg);
	}

	public void unsubscribe(String topic) {
		UnsubscribeMsg msg = new UnsubscribeMsg(user, topic);
		send(msg);
	}

	public void publish(String topic, String message) {
		PublishMsg msg = new PublishMsg(user, topic, message);
		send(msg);
	}

	public void createTopic(String topic) {
		CreateTopicMsg msg = new CreateTopicMsg(user, topic);
		send(msg);
	}

	public void deleteTopic(String topic) {
		DeleteTopicMsg msg = new DeleteTopicMsg(user, topic);
		send(msg);
	}
}
