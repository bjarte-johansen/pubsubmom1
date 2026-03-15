package no.hvl.dat110.iotsystem;

import no.hvl.dat110.client.Client;
import no.hvl.dat110.common.TODO;
import no.hvl.dat110.messages.PublishMsg;

import java.io.IOException;

public class TemperatureDevice {

	private static final int COUNT = 10;

	public static void main(String[] args) {

		// simulated / virtual temperature sensor
		TemperatureSensor sn = new TemperatureSensor();

		// TODO - start

        // create a client object and use it to
        // - connect to the broker - user "sensor" as the user name
        // - publish the temperature(s)
        // - disconnect from the broker

        Client client = new Client("sensor", Common.BROKERHOST, Common.BROKERPORT);
        client.connect();

        for(int i=0; i<COUNT; i++) {

            int temp = sn.read();
            System.out.println("READING[" + i + "]:" + String.valueOf(temp));

            client.publish("temperature", String.valueOf(temp));

            try {
                Thread.sleep(500);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

        client.disconnect();

		// TODO - end

		System.out.println("Temperature device stopping ... ");

		//throw new UnsupportedOperationException(TODO.method());

	}
}
