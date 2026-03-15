package no.hvl.dat110.iotsystem.tests;

import static org.junit.jupiter.api.Assertions.*;

import no.hvl.dat110.alotofnewstuff.Interruptable;
import org.junit.jupiter.api.Test;

import no.hvl.dat110.broker.BrokerServer;
import no.hvl.dat110.iotsystem.DisplayDevice;
import no.hvl.dat110.iotsystem.TemperatureDevice;



public class TestIoTSystem {

	@Test
	public void test() throws InterruptedException{

		System.out.println("IoT system starting ...");

		Runnable display = () -> DisplayDevice.main(null);
		Runnable sensor = () -> TemperatureDevice.main(null);
		Runnable broker = () -> BrokerServer.main(null);

		Thread displaythread = new Thread(display);
		Thread sensorthread = new Thread(sensor);
		Thread brokerthread = new Thread(broker);

		System.out.println("Starting broker ...");

		brokerthread.start();

		// allow broker to reaching waiting for incoming connections
        Thread.sleep(5000);

		System.out.println("Starting display ...");
		
		displaythread.start();

		// allow display to create topic
        Thread.sleep(5000);

        // allow display to subscribe to topic
		System.out.println("Starting sensor ...");
		sensorthread.start();

		try {
			displaythread.join();
			sensorthread.join();
		} catch (Exception e) {
			e.printStackTrace();
		}

		// allow broker thread to finish processing
        Thread.sleep(5000);

		// we check only termination here
		assertTrue(true);

		System.out.println("IoT system stopping ...");
	}
}
