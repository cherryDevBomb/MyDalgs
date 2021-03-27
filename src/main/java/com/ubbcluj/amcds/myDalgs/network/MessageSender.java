package com.ubbcluj.amcds.myDalgs.network;

import com.ubbcluj.amcds.myDalgs.communication.CommunicationProtocol;
import com.ubbcluj.amcds.myDalgs.globals.ProcessConstants;

import java.io.DataOutputStream;
import java.io.IOException;
import java.net.Socket;
import java.util.UUID;

public class MessageSender {

    public static void send(CommunicationProtocol.Message message, CommunicationProtocol.ProcessId sender, String receiverHost, int receiverPort) {
        CommunicationProtocol.NetworkMessage networkMessage = CommunicationProtocol.NetworkMessage
                .newBuilder()
                .setSenderHost(sender.getHost())
                .setSenderListeningPort(sender.getPort())
                .setMessage(message)
                .build();

        CommunicationProtocol.Message outerMessage = CommunicationProtocol.Message
                .newBuilder()
                .setType(CommunicationProtocol.Message.Type.NETWORK_MESSAGE)
                .setNetworkMessage(networkMessage)
//                .setFromAbstractionId(message.getFromAbstractionId())
                .setToAbstractionId(message.getToAbstractionId())
                .setSystemId(ProcessConstants.SYSTEM_ID)
                .setMessageUuid(UUID.randomUUID().toString())
                .build();

        byte[] serializedMessage = outerMessage.toByteArray();

        try (Socket socket = new Socket(receiverHost, receiverPort);
             DataOutputStream dataOutputStream = new DataOutputStream(socket.getOutputStream());
        ) {
            dataOutputStream.writeInt(serializedMessage.length);
            dataOutputStream.write(serializedMessage);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
