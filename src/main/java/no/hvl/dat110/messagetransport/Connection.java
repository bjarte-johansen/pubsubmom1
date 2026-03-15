package no.hvl.dat110.messagetransport;


import no.hvl.dat110.common.Logger;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.Socket;


public class Connection {

	private DataOutputStream outStream; // for writing bytes to the TCP connection
	private DataInputStream inStream; // for reading bytes from the TCP connection
	private Socket socket; // socket for the underlying TCP connection

	public Connection(Socket socket) {

		try {

			this.socket = socket;

			outStream = new DataOutputStream(socket.getOutputStream());

			inStream = new DataInputStream (socket.getInputStream());

		} catch (IOException ex) {

			Logger.log("Connection: " + ex.getMessage());
			ex.printStackTrace();
		}
	}

	public void send(TransportMessage message) {

		// TODO 
		// encapsulate the data contained in the message and write to the output stream
		
		try {

			byte[] sendbuf = message.encapsulate();

			outStream.write(sendbuf);

		} catch (IOException ex) {

			System.out.println("Connection: " + ex.getMessage());
			ex.printStackTrace();
		}

	}

    public boolean isClosed(){

        return socket.isClosed();
    }
/*
	public boolean hasData () {
		boolean hasdata = false;
		
		try {
			hasdata = inStream.available() > 0;
		} catch (IOException ex) {
			System.out.println("Connection: " + ex.getMessage());
			ex.printStackTrace();
		}
		
		return hasdata;
	}
*/


	public TransportMessage receive() throws IOException {
        TransportMessage message;
        byte[] recvbuf;
        int total = 0;

        // TODO
        // read a segment from the input stream and decapsulate into message
        // NOTE: we played a bit and got help from chatgpt to rewrite a little bit cause something
        // was off with the way we read from the stream, we need to read until we have read the
        // whole segment, not just one read call

        recvbuf = new byte[MessageConfig.SEGMENTSIZE];

        while (total < recvbuf.length) {
            int read = inStream.read(recvbuf, total, recvbuf.length - total);
            if (read < 0) throw new IOException("stream closed");

            total += read;
        }

        message = new TransportMessage();
        message.decapsulate(recvbuf);

        return message;
	}

	// close the connection by closing streams and the underlying socket
	public void close() {
		try {
			outStream.close();
			inStream.close();
			socket.close();
		} catch (IOException ex) {

			System.out.println("Connection: " + ex.getMessage());
			ex.printStackTrace();
		}
	}
}