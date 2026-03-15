package no.hvl.dat110.broker;

import java.util.Set;

import no.hvl.dat110.deprecated.PendingMessage;
import no.hvl.dat110.alotofnewstuff.UserState;
import no.hvl.dat110.common.Logger;
import no.hvl.dat110.messages.*;
import no.hvl.dat110.messagetransport.Connection;

public class Dispatcher{
    static private Dispatcher instance;

	private final Storage storage = Storage.getInstance();

    public static Dispatcher getInstance() {
        if(instance == null) {
            instance = new Dispatcher(Storage.getInstance());
            //throw new IllegalStateException("Dispatcher has not been initialized");
        }
        return instance;
    }

	private Dispatcher(Storage storage) {
		//this.storage = storage;

        //this.instance = this;
	}

	public void dispatch(ClientSession client, Message msg) {
		MessageType type = msg.getType();

		// invoke the appropriate handler method
		switch (type) {
            case ACK:
                onAck((AckMsg) msg);
                break;

            case DISCONNECT:
                onDisconnect((DisconnectMsg) msg);
                break;

            case CREATETOPIC:
                onCreateTopic((CreateTopicMsg) msg);
                break;

            case DELETETOPIC:
                onDeleteTopic((DeleteTopicMsg) msg);
                break;

            case SUBSCRIBE:
                onSubscribe((SubscribeMsg) msg);
                break;

            case UNSUBSCRIBE:
                onUnsubscribe((UnsubscribeMsg) msg);
                break;

            case PUBLISH:
                onPublish((PublishMsg) msg);
                break;

            default:
                Logger.log("broker dispatch - unhandled message type {" + msg + "}");

                break;
		}
	}

	// called from Broker after having established the underlying connection
	public void onConnect(ConnectMsg msg, Connection connection, Dispatcher dispatcher) {

		String user = msg.getUser();

		Logger.log("onConnect:" + msg.toString());

		storage.addClientSession(user, connection, dispatcher);

	}

	// called by dispatch upon receiving a disconnect message
	public void onDisconnect(DisconnectMsg msg) {

		String user = msg.getUser();

		Logger.log("onDisconnect:" + msg.toString());

        // debug
		storage.removeClientSession(user);

	}

	public void onCreateTopic(CreateTopicMsg msg) {

		Logger.log("onCreateTopic:" + msg.toString());

		// create the topic in the broker storage
		// the topic is contained in the create topic message

        storage.createTopic(msg.getTopic());
	}

	public void onDeleteTopic(DeleteTopicMsg msg) {

		Logger.log("onDeleteTopic:" + msg.toString());

		// delete the topic from the broker storage
		// the topic is contained in the delete topic message

        storage.deleteTopic(msg.getTopic());
	}

	public void onSubscribe(SubscribeMsg msg) {

		Logger.log("onSubscribe:" + msg.toString());

		// subscribe user to the topic
		// user and topic is contained in the subscribe message

        storage.addSubscriber(msg.getUser(), msg.getTopic());
	}

	public void onUnsubscribe(UnsubscribeMsg msg) {

		Logger.log("onUnsubscribe:" + msg.toString());

		// unsubscribe user to the topic
		// user and topic is contained in the unsubscribe message

        storage.removeSubscriber(msg.getUser(), msg.getTopic());
	}

	public void onPublish(PublishMsg msg) {

		Logger.log("onPublish:" + msg.toString());

		// publish the message to clients subscribed to the topic
		//      topic and message is contained in the subscribe message
		//      messages must be sent using the corresponding client session objects

        Set<String> subscribers = storage.getSubscribers(msg.getTopic());
        if(subscribers != null) {
            for(String subscriber : subscribers) {
                try(var ignored = Logger.scope("Publish message to subscriber")) {

                    UserState userState = storage.getUserState(subscriber);
                    ClientSession session = userState.getSession();

                    if(session != null) {
                        // send
                        Logger.log("onPublish: sendReliable to user " + subscriber + ".");
                        session.sendReliable(msg);
                    } else {
                        // subscriber is not logged in, add message to mailbox
                        Logger.log("onPublish: subscriber not logged in " + subscriber + ". Message added to mailbox.");
                        userState.addToMailbox(msg);
                    }
                }

            }
        }
	}

    public void onAck(AckMsg msg) {
        Logger.log("Unhandled onAck:" + msg.toString());
    }
}
