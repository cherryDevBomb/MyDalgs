package com.ubbcluj.amcds.myDalgs.network;

import java.io.DataInputStream;
import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;

public class MessageReceiver implements Runnable {

    private final int processPort;

    public MessageReceiver(int processPort) {
        this.processPort = processPort;
    }

    @Override
    public void run() {
        try (ServerSocket serverSocket = new ServerSocket(processPort)) {
            System.out.println("Waiting for requests on port " + processPort);
            while (true) {
                //TODO extract to separate method to use try with resources
                Socket clientSocket = serverSocket.accept();
                DataInputStream dataInputStream = new DataInputStream(clientSocket.getInputStream());
                System.out.println("Getting messages and adding to queue for port " + processPort);



                clientSocket.close();
                dataInputStream.close();
                break;
            }
        }
        catch (IOException e) {
            e.printStackTrace();
        }
    }
}
