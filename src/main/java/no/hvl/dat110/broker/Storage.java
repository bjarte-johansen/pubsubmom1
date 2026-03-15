package no.hvl.dat110.broker;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

import no.hvl.dat110.deprecated.PendingMessage;
import no.hvl.dat110.alotofnewstuff.UserState;
import no.hvl.dat110.messages.Message;
import no.hvl.dat110.messages.PublishMsg;
import no.hvl.dat110.messagetransport.Connection;



public class Storage {

    public static final Storage instance = new Storage();

    // singleton system was not needed, but we didnt remove it before handing in the assignment, so we just kept
    // it to avoid breaking code in other classes that use it (TEST CASES ONLY)
    // the reset method can be used to reset the storage to an empty state between test cases

    public static Storage getInstance() {
        return instance;
    }

    public Storage reset(){
        subscriptions.clear();
        userStates.clear();
        return this;
    }

    //public record UndeliveredMessage(String topic, String message) {};

	// data structure for managing subscriptions
	// maps from a topic to set of subscribed users
	protected ConcurrentHashMap<String, Set<String>> subscriptions = new ConcurrentHashMap<String, Set<String>>();


	// data structure for managing currently connected clients
	// maps from user to corresponding client session object
	protected ConcurrentHashMap<String, UserState> userStates = new ConcurrentHashMap<String, UserState>();


    // hidden constructor
	private Storage() {}


    /*
    getSessions() is no longer releveant, but just kept as an alias for getUserStates() to avoid breaking code in
    other classes that use it (TEST CASES ONLY)
     */
	public Collection<UserState> getSessions() {
        return getUserStates();
    }
    public Collection<UserState> getUserStates() {
        Collection<UserState> states = userStates.values();
        return states;
    }

	public Set<String> getTopics() {
		return subscriptions.keySet();
	}

	// get the session object for a given user
	// session object can be used to send a message to the user
	
	public UserState getUserState(String user) {
        UserState state = userStates.get(user);
		return state;
	}
/*
    public UserState getState(String user) {
        UserState state = userStates.get(user);
        return state;
    }
*/

	public Set<String> getSubscribers(String topic) {
        Set<String> subscribers = subscriptions.get(topic);
		return subscribers;
	}

	public void addClientSession(String user, Connection connection, Dispatcher dispatcher) {

		// TODO: add corresponding client session to the storage
		// See ClientSession class

        UserState state = userStates.computeIfAbsent(user, s -> new UserState());

        // set session for the user
        ClientSession session = new ClientSession(user, connection, dispatcher);
        state.setSession(session);
        session.start();

        // send undelivered messages to the user if there are any
        sendUndeliveredMessages(user);
	}

	public void removeClientSession(String user) {
		// disconnet the client (user) and remove client session for user from the storage
        UserState state = userStates.get(user);
        if(state != null) {
            // disconnect
            state.session.disconnect();

            // drain pending messages for the user and add them to the mailbox so they can be sent when the user connects again
            Collection<Message> pending = state.session.reliableSender.drainPending()
                .stream()
                .map(PendingMessage::getMessage)
                .toList();
            state.addToMailbox(pending);

            // clear session for the user
            state.setSession(null);
        }

        userStates.remove(user);
	}

	public void createTopic(String topic) {
		// create topic in the storage
        subscriptions.put(topic, ConcurrentHashMap.newKeySet());
	}

	public void deleteTopic(String topic) {
		// delete topic from the storage
        subscriptions.remove(topic);
	}

	public void addSubscriber(String user, String topic) {
		// add the user as subscriber to the topic
        subscriptions.computeIfAbsent(topic, k -> ConcurrentHashMap.newKeySet()).add(user);
	}

	public void removeSubscriber(String user, String topic) {
		// remove the user as subscriber to the topic
        if(subscriptions.containsKey(topic)) {
            subscriptions.get(topic).remove(user);
        }

        /*
        // prefer to use computeIfPresent to avoid potential race condition where topic is deleted between the
        // containsKey and get calls, but it is a bit more verbose
        subscriptions.computeIfPresent(topic, (k, v) -> {
            subscriptions.get(topic).remove(user);
            return v;
        });
        */
	}


    /**
     * This method is used to send undelivered messages to the user when they connect
     */
    protected void sendUndeliveredMessages(String user) {
        // get the user state for the user
        UserState userState = userStates.get(user);

        // send any undelivered messages to the user
        // sort by message ID to ensure messages are sent in the order they were published
        // NOTE: this is not strictly perfect as messageId could wrap around, but for simplicity we
        // assume that messageId is always increasing and never wraps around in this implementation

        List<Message> unsentMessages = userState.getMailboxMessages()
            .stream()
            .sorted(Comparator.comparing(Message::getMessageId))
            .toList();

        if(!unsentMessages.isEmpty()) {
            System.out.println("Sending mailbox (" + unsentMessages.size() + " messages) to user " + user);

            unsentMessages.forEach(userState.session::sendReliable);
        }
    }
}
