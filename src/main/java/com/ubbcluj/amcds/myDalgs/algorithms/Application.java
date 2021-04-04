package com.ubbcluj.amcds.myDalgs.algorithms;

import com.ubbcluj.amcds.myDalgs.Process;
import com.ubbcluj.amcds.myDalgs.communication.Protocol;
import com.ubbcluj.amcds.myDalgs.globals.AbstractionType;
import com.ubbcluj.amcds.myDalgs.util.AbstractionIdUtil;

public class Application extends Abstraction {

    public Application(String abstractionId, Process process) {
        super(abstractionId, process);
        process.registerAbstraction(new BestEffortBroadcast(AbstractionIdUtil.getChildAbstractionId(abstractionId, AbstractionType.BEB), process));
        process.registerAbstraction(new PerfectLink(AbstractionIdUtil.getChildAbstractionId(abstractionId, AbstractionType.PL), process));

    }

    @Override
    public boolean handle(Protocol.Message message) {
        if (Protocol.Message.Type.PL_DELIVER.equals(message.getType())) {
            message = message.getPlDeliver().getMessage();
        }

        switch (message.getType()) {
            case APP_BROADCAST:
                handleAppBroadcast(message.getAppBroadcast(), message.getSystemId());
                return true;
            case BEB_DELIVER:
                Protocol.Message innerMessage = message.getBebDeliver().getMessage();
                if (Protocol.Message.Type.APP_VALUE.equals(innerMessage.getType())) {
                    handleBebDeliver(innerMessage);
                    return true;
                }
        }
        return false;
    }

    private void handleAppBroadcast(Protocol.AppBroadcast appBroadcast, String systemId) {
        Protocol.AppValue appValue = Protocol.AppValue
                .newBuilder()
                .setValue(appBroadcast.getValue())
                .build();

        Protocol.Message appValueMessage = Protocol.Message
                .newBuilder()
                .setType(Protocol.Message.Type.APP_VALUE)
                .setAppValue(appValue)
                .setFromAbstractionId(this.abstractionId)
                .setToAbstractionId(this.abstractionId)
                .setSystemId(systemId)
                .build();

        Protocol.BebBroadcast bebBroadcast = Protocol.BebBroadcast
                .newBuilder()
                .setMessage(appValueMessage)
                .build();

        Protocol.Message bebBroadcastMessage = Protocol.Message
                .newBuilder()
                .setType(Protocol.Message.Type.BEB_BROADCAST)
                .setBebBroadcast(bebBroadcast)
                .setFromAbstractionId(this.abstractionId)
                .setToAbstractionId(AbstractionIdUtil.getChildAbstractionId(this.abstractionId, AbstractionType.BEB))
                .setSystemId(systemId)
                .build();

        process.addMessageToQueue(bebBroadcastMessage);
    }


    private void handleBebDeliver(Protocol.Message appValueMessage) {
        Protocol.PlSend plSend = Protocol.PlSend
                .newBuilder()
                .setDestination(process.getHub())
                .setMessage(appValueMessage)
                .build();

        Protocol.Message plSendMessage = Protocol.Message
                .newBuilder()
                .setType(Protocol.Message.Type.PL_SEND)
                .setPlSend(plSend)
                .setFromAbstractionId(this.abstractionId)
                .setToAbstractionId(AbstractionIdUtil.getChildAbstractionId(this.abstractionId, AbstractionType.PL))
                .setSystemId(appValueMessage.getSystemId())
                .build();

        process.addMessageToQueue(plSendMessage);
    }
}
