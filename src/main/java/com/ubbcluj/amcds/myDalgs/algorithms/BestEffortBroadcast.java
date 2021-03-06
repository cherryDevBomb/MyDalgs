package com.ubbcluj.amcds.myDalgs.algorithms;

import com.ubbcluj.amcds.myDalgs.Process;
import com.ubbcluj.amcds.myDalgs.communication.Protocol;
import com.ubbcluj.amcds.myDalgs.model.AbstractionType;
import com.ubbcluj.amcds.myDalgs.util.AbstractionIdUtil;

public class BestEffortBroadcast extends Abstraction {

    public BestEffortBroadcast(String abstractionId, Process process) {
        super(abstractionId, process);
        process.registerAbstraction(new PerfectLink(AbstractionIdUtil.getChildAbstractionId(abstractionId, AbstractionType.PL), process));
    }

    @Override
    public boolean handle(Protocol.Message message) {
        switch (message.getType()) {
            case BEB_BROADCAST:
                handleBebBroadcast(message.getBebBroadcast());
                return true;
            case PL_DELIVER:
                    triggerBebDeliver(message.getPlDeliver().getMessage(), message.getPlDeliver().getSender());
                    return true;
        }
        return false;
    }

    private void handleBebBroadcast(Protocol.BebBroadcast bebBroadcast) {
        process.getProcesses().forEach(p -> {
            Protocol.PlSend plSend = Protocol.PlSend
                    .newBuilder()
                    .setDestination(p)
                    .setMessage(bebBroadcast.getMessage())
                    .build();

            Protocol.Message plSendMessage = Protocol.Message
                    .newBuilder()
                    .setType(Protocol.Message.Type.PL_SEND)
                    .setPlSend(plSend)
                    .setFromAbstractionId(this.abstractionId)
                    .setToAbstractionId(AbstractionIdUtil.getChildAbstractionId(this.abstractionId, AbstractionType.PL))
                    .setSystemId(process.getSystemId())
                    .build();

            process.addMessageToQueue(plSendMessage);
        });
    }

    private void triggerBebDeliver(Protocol.Message appValueMessage, Protocol.ProcessId sender) {
        Protocol.BebDeliver bebDeliver = Protocol.BebDeliver
                .newBuilder()
                .setMessage(appValueMessage)
                .setSender(sender)
                .build();

        Protocol.Message bebDeliverMessage = Protocol.Message
                .newBuilder()
                .setType(Protocol.Message.Type.BEB_DELIVER)
                .setBebDeliver(bebDeliver)
                .setFromAbstractionId(this.abstractionId)
                .setToAbstractionId(AbstractionIdUtil.getParentAbstractionId(this.abstractionId))
                .setSystemId(process.getSystemId())
                .build();

        process.addMessageToQueue(bebDeliverMessage);
    }
}
