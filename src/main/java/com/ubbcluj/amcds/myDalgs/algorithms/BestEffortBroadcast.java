package com.ubbcluj.amcds.myDalgs.algorithms;

import com.ubbcluj.amcds.myDalgs.Process;
import com.ubbcluj.amcds.myDalgs.communication.Protocol;
import com.ubbcluj.amcds.myDalgs.globals.AbstractionType;
import com.ubbcluj.amcds.myDalgs.network.MessageSender;
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
                    .build();

            process.addMessageToQueue(plSendMessage);
        });
    }
}
