package no.hvl.dat110.broker;

import no.hvl.dat110.common.Logger;

import java.util.Objects;

public class BrokerServer extends Thread {

	private static int BROKER_DEFAULTPORT = 8080;
	
	public static void main(String[] args) {
		int port = BROKER_DEFAULTPORT;
		
		if (args != null && args.length > 0 && Objects.nonNull(args[0])) {
    		port = Integer.parseInt(args[0]);
		}

		Logger.log("Broker server : " + port);
		
		//Storage storage = new Storage();
        // reset storage instance
        Storage storage = Storage
            .getInstance()
            .reset();
		Dispatcher dispatcher = Dispatcher.getInstance();

		Broker broker = new Broker(dispatcher, port);
		
		// start dispatcher and broker threads
		//dispatcher.start();
		broker.start();
		
		// wait for termination of dispatcher and broker threads before stopping broker server
		try {
			broker.join();
			//dispatcher.join();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		
		Logger.log("Broker server stopping ... ");
	}
}
