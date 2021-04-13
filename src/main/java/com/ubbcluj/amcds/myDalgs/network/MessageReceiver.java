package com.ubbcluj.amcds.myDalgs.network;

import com.ubbcluj.amcds.myDalgs.communication.Protocol;

import java.io.DataInputStream;
import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Observable;

public class MessageReceiver extends Observable implements Runnable {

    private final int processPort;

    public MessageReceiver(int processPort) {
        this.processPort = processPort;
    }

    @Override
    public void run() {
        try (ServerSocket serverSocket = new ServerSocket(processPort)) {
            System.out.println("Waiting for requests on port " + processPort);
            while (true) {
                receiveMessage(serverSocket);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void receiveMessage(ServerSocket serverSocket) throws IOException {
        try (Socket clientSocket = serverSocket.accept();
             DataInputStream dataInputStream = new DataInputStream(clientSocket.getInputStream());
        ) {
            int messageSize = dataInputStream.readInt();
            byte[] byteBuffer = new byte[messageSize];
            int readMessageSize = dataInputStream.read(byteBuffer, 0, messageSize);

            if (messageSize != readMessageSize) {
                throw new RuntimeException("Network message has incorrect size: expected = " + messageSize + ", actual = " + readMessageSize);
            }

            Protocol.Message message = Protocol.Message.parseFrom(byteBuffer);

            if (!Protocol.Message.Type.NETWORK_MESSAGE.equals(message.getType())) {
                throw new RuntimeException("Network message has incorrect type: expected = " + Protocol.Message.Type.NETWORK_MESSAGE + ", actual = " + message.getType());
            }

            setChanged();
            notifyObservers(message);
        }
    }
}
