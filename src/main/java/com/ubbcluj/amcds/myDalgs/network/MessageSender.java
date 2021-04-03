package com.ubbcluj.amcds.myDalgs.network;

import com.ubbcluj.amcds.myDalgs.communication.Protocol;

import java.io.DataOutputStream;
import java.io.IOException;
import java.net.Socket;

public class MessageSender {

    public static void send(Protocol.Message message, String receiverHost, int receiverPort) {
        byte[] serializedMessage = message.toByteArray();

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
