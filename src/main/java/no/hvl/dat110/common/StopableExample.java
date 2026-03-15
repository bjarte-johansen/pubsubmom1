package no.hvl.dat110.common;

public class StopableExample extends Stopable {

	public StopableExample() {
		super("stopable thread");
	}

	private int i = 0;

	@Override
	public void doProcess() {

		Logger.log("stopable thread working:" + i);

		try {

			// simulate some processing time
			Thread.sleep(1000);

		} catch (InterruptedException ex) {

            Logger.log("Stopable thread " + ex.getMessage());
			ex.printStackTrace();
		}
		
		i++;
	}
}
