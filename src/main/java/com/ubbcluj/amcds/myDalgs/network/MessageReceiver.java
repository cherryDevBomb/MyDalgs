package com.ubbcluj.amcds.myDalgs.network;

import com.ubbcluj.amcds.myDalgs.communication.Protocol;
import com.ubbcluj.amcds.myDalgs.util.IncomingMessageWrapper;

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
                break;
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void receiveMessage(ServerSocket serverSocket) throws IOException {
        try (Socket clientSocket = serverSocket.accept();
             DataInputStream dataInputStream = new DataInputStream(clientSocket.getInputStream());
        ) {
            System.out.println("Getting messages and adding to queue for port " + processPort);

            int messageSize = dataInputStream.readInt();
            byte[] byteBuffer = new byte[messageSize];
            int readMessageSize = dataInputStream.read(byteBuffer, 0, messageSize);

            if (messageSize != readMessageSize) {
                //TODO implement stubborn read
                throw new RuntimeException("Network message has incorrect size: expected = " + messageSize + ", actual = " + readMessageSize);
            }

            Protocol.Message message = Protocol.Message.parseFrom(byteBuffer);

            if (!Protocol.Message.Type.NETWORK_MESSAGE.equals(message.getType())) {
                throw new RuntimeException("Network message has incorrect type: expected = " + Protocol.Message.Type.NETWORK_MESSAGE + ", actual = " + message.getType());
            }

            Protocol.NetworkMessage networkMessage = message.getNetworkMessage();
            Protocol.Message innerMessage = networkMessage.getMessage();

            IncomingMessageWrapper messageWrapper = new IncomingMessageWrapper(innerMessage, networkMessage.getSenderListeningPort(), message.getToAbstractionId());
            setChanged();
            notifyObservers(messageWrapper);
        }
    }
}
