package no.hvl.dat110.broker.processing.tests;

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import no.hvl.dat110.broker.Broker;
import no.hvl.dat110.broker.Dispatcher;
import no.hvl.dat110.broker.Storage;

public abstract class Test0Base {

	// TODO: many possibilities for better testing
	protected Dispatcher dispatcher;
	protected Broker broker;
	protected Storage storage;
	
	protected int BROKER_TESTPORT = 0;
	protected String BROKER_TESTHOST = "localhost";

    // we change teardown delay to allow the system to run for a while before shutting down,
    // which can be useful for debugging and observing the system behavior
    protected int TEARDOWN_DELAY = 5000;
    protected int WAIT_FOR_CLIENTS_INTERVAL = 3000;

	protected int RUNTIME = 10000; // time to allow test to execute

    public int GET_LISTENING_PORT(){
        return broker.getMessagingServer().getListeningPort();
    }

	
	@BeforeEach
	public void setUp() throws Exception {

        // reset storage instance
        Storage storage = Storage
            .getInstance()
            .reset();
		dispatcher = Dispatcher.getInstance();
		broker = new Broker(dispatcher, BROKER_TESTPORT);
		
		//dispatcher.start();
		broker.start();
		
		// allow broker to reaching waiting for incoming connections
		try {
			Thread.sleep(WAIT_FOR_CLIENTS_INTERVAL);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}

	@AfterEach
	public void tearDown() throws Exception {
        // proper order: stop(signal to stop run) -> join() (wait untill run is done)
        broker.doStop();

		try {
            // we dont need this
			//Thread.sleep(TEARDOWN_DELAY);

            // let the system run for a while
			broker.join();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}

	@Test
	public void test() {
		fail("Not yet implemented");
	}

}
