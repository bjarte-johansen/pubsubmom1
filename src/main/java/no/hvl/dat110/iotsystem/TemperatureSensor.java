package no.hvl.dat110.iotsystem;

public class TemperatureSensor {

	private static final int RANGE = 20;

	public int read() {

		long seconds = System.currentTimeMillis();

		double temp = RANGE * Math.sin((double) seconds / 1000.0);

		return (int) Math.ceil(temp);
	}
}
