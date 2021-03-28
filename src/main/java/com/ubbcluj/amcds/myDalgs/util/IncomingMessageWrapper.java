package com.ubbcluj.amcds.myDalgs.util;

import com.ubbcluj.amcds.myDalgs.communication.Protocol;

public class IncomingMessageWrapper {

    private final Protocol.Message message;
    private final int senderPort;
    private final String toAbstractionId;

    public IncomingMessageWrapper(Protocol.Message message, int senderPort, String toAbstractionId) {
        this.message = message;
        this.senderPort = senderPort;
        this.toAbstractionId = toAbstractionId;
    }

    public Protocol.Message getMessage() {
        return message;
    }

    public int getSenderPort() {
        return senderPort;
    }

    public String getToAbstractionId() {
        return toAbstractionId;
    }
}
