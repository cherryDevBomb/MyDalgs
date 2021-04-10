package com.ubbcluj.amcds.myDalgs.algorithms;

import com.ubbcluj.amcds.myDalgs.Process;
import com.ubbcluj.amcds.myDalgs.communication.Protocol;
import com.ubbcluj.amcds.myDalgs.model.AbstractionType;
import com.ubbcluj.amcds.myDalgs.util.AbstractionIdUtil;

public class Application extends Abstraction {

    public Application(String abstractionId, Process process) {
        super(abstractionId, process);
        process.registerAbstraction(new BestEffortBroadcast(AbstractionIdUtil.getChildAbstractionId(abstractionId, AbstractionType.BEB), process));
        process.registerAbstraction(new PerfectLink(AbstractionIdUtil.getChildAbstractionId(abstractionId, AbstractionType.PL), process));
    }

    @Override
    public boolean handle(Protocol.Message message) {
        // TODO refactor into nested switch
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
                    triggerPlSend(innerMessage);
                    return true;
                }
                System.out.println("WARN: beb deliver not app_value");
            case APP_WRITE:
                handleAppWrite(message.getAppWrite(), message.getSystemId());
                return true;
            case NNAR_READ_RETURN:
                handleNnarReadReturn(message.getNnarReadReturn(), message.getFromAbstractionId(), message.getSystemId());
                return true;
            case NNAR_WRITE_RETURN:
                handleNnarWriteReturn(message.getNnarWriteReturn(), message.getFromAbstractionId(), message.getSystemId());
                return true;
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

    private void handleAppWrite(Protocol.AppWrite appWrite, String systemId) {
        // register app.nnar[register] abstraction if not present
        String nnarAbstractionId = AbstractionIdUtil.getNamedAbstractionId(this.abstractionId, AbstractionType.NNAR, appWrite.getRegister());
        process.registerAbstraction(new NNAtomicRegister(nnarAbstractionId, process));

        Protocol.NnarWrite nnarWrite = Protocol.NnarWrite
                .newBuilder()
                .setValue(appWrite.getValue())
                .build();

        Protocol.Message nnarWriteMessage = Protocol.Message
                .newBuilder()
                .setType(Protocol.Message.Type.NNAR_WRITE)
                .setNnarWrite(nnarWrite)
                .setFromAbstractionId(this.abstractionId)
                .setToAbstractionId(nnarAbstractionId)
                .setSystemId(systemId)
                .build();

        process.addMessageToQueue(nnarWriteMessage);
    }


    private void handleNnarReadReturn(Protocol.NnarReadReturn nnarReadReturn, String fromAbstractionId, String systemId) {
        Protocol.AppReadReturn appReadReturn = Protocol.AppReadReturn
                .newBuilder()
                .setRegister(AbstractionIdUtil.getInternalNameFromAbstractionId(fromAbstractionId))
                .setValue(nnarReadReturn.getValue())
                .build();

        Protocol.Message appReadReturnMessage = Protocol.Message
                .newBuilder()
                .setType(Protocol.Message.Type.APP_READ_RETURN)
                .setAppReadReturn(appReadReturn)
                .setFromAbstractionId(this.abstractionId)
                .setToAbstractionId(AbstractionIdUtil.HUB_ID)
                .setSystemId(systemId)
                .build();

        triggerPlSend(appReadReturnMessage);
    }

    private void handleNnarWriteReturn(Protocol.NnarWriteReturn nnarWriteReturn, String fromAbstractionId, String systemId) {
        Protocol.AppWriteReturn appWriteReturn = Protocol.AppWriteReturn
                .newBuilder()
                .setRegister(AbstractionIdUtil.getInternalNameFromAbstractionId(fromAbstractionId))
                .build();

        Protocol.Message appWriteReturnMessage = Protocol.Message
                .newBuilder()
                .setType(Protocol.Message.Type.APP_WRITE_RETURN)
                .setAppWriteReturn(appWriteReturn)
                .setFromAbstractionId(this.abstractionId)
                .setToAbstractionId(AbstractionIdUtil.HUB_ID)
                .setSystemId(systemId)
                .build();

        triggerPlSend(appWriteReturnMessage);
    }

    private void triggerPlSend(Protocol.Message message) {
        Protocol.PlSend plSend = Protocol.PlSend
                .newBuilder()
                .setDestination(process.getHub())
                .setMessage(message)
                .build();

        Protocol.Message plSendMessage = Protocol.Message
                .newBuilder()
                .setType(Protocol.Message.Type.PL_SEND)
                .setPlSend(plSend)
                .setFromAbstractionId(this.abstractionId)
                .setToAbstractionId(AbstractionIdUtil.getChildAbstractionId(this.abstractionId, AbstractionType.PL))
                .setSystemId(message.getSystemId())
                .build();

        process.addMessageToQueue(plSendMessage);
    }
}
