package com.ubbcluj.amcds.myDalgs.algorithms;

import com.ubbcluj.amcds.myDalgs.communication.Protocol;
import com.ubbcluj.amcds.myDalgs.network.MessageSender;
import com.ubbcluj.amcds.myDalgs.util.AbstractionIdUtil;

import java.util.Optional;
import java.util.UUID;

public class PerfectLink extends Abstraction {

    public PerfectLink(String abstractionId, com.ubbcluj.amcds.myDalgs.Process process) {
        super(abstractionId, process);
    }

    @Override
    public boolean handle(Protocol.Message message) {
        switch (message.getType()) {
            case PL_SEND:
                handlePlSend(message.getPlSend(), message.getToAbstractionId(), message.getSystemId());
                return true;
            case NETWORK_MESSAGE:
                triggerPlDeliver(message.getNetworkMessage(), AbstractionIdUtil.getParentAbstractionId(message.getToAbstractionId()));
                return true;
        }
        return false;
    }

    private void handlePlSend(Protocol.PlSend plSendMessage, String toAbstractionId, String systemId) {
        Protocol.ProcessId sender = process.getProcess();
        Protocol.ProcessId destination = plSendMessage.getDestination();

        Protocol.NetworkMessage networkMessage = Protocol.NetworkMessage
                .newBuilder()
                .setSenderHost(sender.getHost())
                .setSenderListeningPort(sender.getPort())
                .setMessage(plSendMessage.getMessage())
                .build();

        Protocol.Message outerMessage = Protocol.Message
                .newBuilder()
                .setType(Protocol.Message.Type.NETWORK_MESSAGE)
                .setNetworkMessage(networkMessage)
                .setFromAbstractionId(this.abstractionId)
                .setToAbstractionId(toAbstractionId)
                .setSystemId(systemId)
                .setMessageUuid(UUID.randomUUID().toString())
                .build();

        MessageSender.send(outerMessage, destination.getHost(), destination.getPort());
    }


    private void triggerPlDeliver(Protocol.NetworkMessage networkMessage, String toAbstractionId) {
        Optional<Protocol.ProcessId> sender = process.getProcessByHostAndPort(networkMessage.getSenderHost(), networkMessage.getSenderListeningPort());
        Protocol.PlDeliver.Builder plDeliverBuilder = Protocol.PlDeliver
                .newBuilder()
                .setMessage(networkMessage.getMessage());
        sender.ifPresent(plDeliverBuilder::setSender);

        Protocol.PlDeliver plDeliver = plDeliverBuilder.build();

        Protocol.Message message = Protocol.Message
                .newBuilder()
                .setType(Protocol.Message.Type.PL_DELIVER)
                .setPlDeliver(plDeliver)
                .setToAbstractionId(toAbstractionId)
                .setSystemId(networkMessage.getMessage().getSystemId())
                .build();

        process.addMessageToQueue(message);
    }
}
