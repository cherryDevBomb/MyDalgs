package com.ubbcluj.amcds.myDalgs.network;

import com.ubbcluj.amcds.myDalgs.communication.CommunicationProtocol;

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
                receiveMessage(serverSocket);
                break;
            }
        }
        catch (IOException e) {
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

            CommunicationProtocol.Message message = CommunicationProtocol.Message.parseFrom(byteBuffer);

            if (message.getType() != CommunicationProtocol.Message.Type.NETWORK_MESSAGE) {
                throw new RuntimeException("Network message has incorrect type: expected = " + CommunicationProtocol.Message.Type.NETWORK_MESSAGE + ", actual = " + message.getType());
            }

            CommunicationProtocol.NetworkMessage networkMessage = message.getNetworkMessage();

            CommunicationProtocol.Message innerMessage = networkMessage.getMessage();
        }
    }
}
