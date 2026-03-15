package no.hvl.dat110.messagetransport;

import no.hvl.dat110.common.Logger;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;

public class MessagingServer {

	private ServerSocket welcomeSocket;
	
	public MessagingServer(int port) {
		try {
			this.welcomeSocket = new ServerSocket(port);
            this.welcomeSocket.setReuseAddress(true);
            //this.welcomeSocket.bind(new InetSocketAddress("localhost", port));

            Logger.log("Messaging ServerSocket started on port " + port);
		} catch (IOException ex) {
			Logger.log("Messaging server failed to start on port " + port);
			System.out.println("Messaging server: " + ex.getMessage());
			ex.printStackTrace();
		}
	}

    public int getListeningPort() {
        return welcomeSocket.getLocalPort();
    }

	// accept an incoming connection from a client
	public Connection accept () {
		Connection connection = null;
		
		// accept TCP connection on welcome socket and create connection
        Logger.log("Messaging server accepting connection ...");

        if(welcomeSocket != null) {
            Logger.log("Messaging server is listening on port " + welcomeSocket.getLocalPort());
        } else {
            Logger.log("Messaging server welcome socket is null");
        }

		try {
			Socket connectionSocket = welcomeSocket.accept();
			connection = new Connection(connectionSocket);

            if(connection == null) {
                Logger.log("Failed to accept connection");
            } else {
                Logger.log("Connection accepted");
            }
		} catch (IOException ex) {
            Logger.log("Messaging server: " + ex.getMessage());
			ex.printStackTrace();
		}
		
		return connection;
	}
	
	public void stop() {
		if (welcomeSocket != null && !welcomeSocket.isClosed()) {
			try {
			    welcomeSocket.close();
                Logger.log("Messaging server stopped");
                throw new IOException("dummy exception to test broker stop");
			} catch (IOException ex) {
                Logger.log("Messaging server: " + ex.getMessage());
                ex.printStackTrace();
		    }
		}
	}

}
