package no.hvl.dat110.broker;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;

import no.hvl.dat110.messages.PublishMsg;
import no.hvl.dat110.messagetransport.Connection;



public class Storage {
    //public record UndeliveredMessage(String topic, String message) {};

	// data structure for managing subscriptions
	// maps from a topic to set of subscribed users
	protected ConcurrentHashMap<String, Set<String>> subscriptions;
	
	// data structure for managing currently connected clients
	// maps from user to corresponding client session object
	
	protected ConcurrentHashMap<String, ClientSession> clients;

    // handle storing messages for users that are not connect
    protected ConcurrentHashMap<String, ConcurrentLinkedQueue<PublishMsg>> undeliveredMessages;

	public Storage() {
		subscriptions = new ConcurrentHashMap<String, Set<String>>();
		clients = new ConcurrentHashMap<String, ClientSession>();
        undeliveredMessages = new ConcurrentHashMap<String, ConcurrentLinkedQueue<PublishMsg>>();
	}

	public Collection<ClientSession> getSessions() {
		return clients.values();
	}

	public Set<String> getTopics() {

		return subscriptions.keySet();

	}

	// get the session object for a given user
	// session object can be used to send a message to the user
	
	public ClientSession getSession(String user) {

		ClientSession session = clients.get(user);

		return session;
	}

	public Set<String> getSubscribers(String topic) {

		return (subscriptions.get(topic));

	}

	public void addClientSession(String user, Connection connection) {

		// TODO: add corresponding client session to the storage
		// See ClientSession class

        clients.put(user, new ClientSession(user, connection));

        // send undelivered messages to the user if there are any
        sendUndeliveredMessages(user);
	}

	public void removeClientSession(String user) {

		// TODO: disconnet the client (user) 
		// and remove client session for user from the storage

        clients.remove(user);
		
		//throw new UnsupportedOperationException(TODO.method());
		
	}

	public void createTopic(String topic) {

		// TODO: create topic in the storage
        subscriptions.put(topic, ConcurrentHashMap.newKeySet());

		//throw new UnsupportedOperationException(TODO.method());
	
	}

	public void deleteTopic(String topic) {

		// TODO: delete topic from the storage
        subscriptions.remove(topic);

		//throw new UnsupportedOperationException(TODO.method());
		
	}

	public void addSubscriber(String user, String topic) {

		// TODO: add the user as subscriber to the topic
        subscriptions.computeIfAbsent(topic, k -> ConcurrentHashMap.newKeySet()).add(user);
		
		//throw new UnsupportedOperationException(TODO.method());
		
	}

	public void removeSubscriber(String user, String topic) {

		// TODO: remove the user as subscriber to the topic
        if(subscriptions.containsKey(topic)) {
            subscriptions.get(topic).remove(user);
        }

        // possible but would create entries that are empty if the topic is not present
        // subscriptions.computeIfAbsent(topic, k -> ConcurrentHashMap.newKeySet()).remove(user);

		//throw new UnsupportedOperationException(TODO.method());
	}

    public void addUndeliveredMessage(String user, PublishMsg msg) {
        undeliveredMessages
            .computeIfAbsent(user, k -> new ConcurrentLinkedQueue<PublishMsg>())
            .add(msg);
    }

    /**
     * This method is used to send undelivered messages to the user when they connect
     */
    public void sendUndeliveredMessages(String user) {
        // get the queue of undelivered messages for the user
        Queue<PublishMsg> queue = undeliveredMessages.get(user);

        // send any undelivered messages to the user
        if(queue != null && !queue.isEmpty()) {
            System.out.println("Sending previously undelivered messages (" + queue.size() + ") to user " + user);

            while (!queue.isEmpty()) {
                PublishMsg msg = queue.poll();
                clients.get(user).send(msg);
            }
        }
    }
}
