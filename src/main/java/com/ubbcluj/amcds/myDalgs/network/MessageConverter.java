package com.ubbcluj.amcds.myDalgs.network;

import com.ubbcluj.amcds.myDalgs.communication.Protocol;

public class MessageConverter {

    public static Protocol.Message createPlDeliverMessage(Protocol.Message message, Protocol.ProcessId sender, String toAbstractionId) {
        Protocol.PlDeliver plDeliverMessage = Protocol.PlDeliver
                .newBuilder()
                .setMessage(message)
                .setSender(sender)
                .build();

        Protocol.Message outerMessage = Protocol.Message
                .newBuilder()
                .setType(Protocol.Message.Type.PL_DELIVER)
                .setPlDeliver(plDeliverMessage)
                .setToAbstractionId(toAbstractionId)
//                .setSystemId() //TODO needed?
                .build();

        return outerMessage;
    }
}
