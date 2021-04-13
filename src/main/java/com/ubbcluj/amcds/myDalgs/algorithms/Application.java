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
        switch (message.getType()) {
            case PL_DELIVER:
                Protocol.PlDeliver plDeliver = message.getPlDeliver();
                switch (plDeliver.getMessage().getType()) {
                    case APP_BROADCAST:
                        handleAppBroadcast(plDeliver.getMessage().getAppBroadcast());
                        return true;
                    case APP_READ:
                        handleAppRead(plDeliver.getMessage().getAppRead());
                        return true;
                    case APP_WRITE:
                        handleAppWrite(plDeliver.getMessage().getAppWrite());
                        return true;
                }
                return false;
            case BEB_DELIVER:
                Protocol.Message innerMessage = message.getBebDeliver().getMessage();
                    triggerPlSend(innerMessage);
                    return true;
            case NNAR_READ_RETURN:
                handleNnarReadReturn(message.getNnarReadReturn(), message.getFromAbstractionId());
                return true;
            case NNAR_WRITE_RETURN:
                handleNnarWriteReturn(message.getNnarWriteReturn(), message.getFromAbstractionId());
                return true;
        }
        System.out.println(message.getType());
        return false;
    }

    private void handleAppBroadcast(Protocol.AppBroadcast appBroadcast) {
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
                .setSystemId(process.getSystemId())
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
                .setSystemId(process.getSystemId())
                .build();

        process.addMessageToQueue(bebBroadcastMessage);
    }

    private void handleAppRead(Protocol.AppRead appRead) {
        String nnarAbstractionId = AbstractionIdUtil.getNamedAbstractionId(this.abstractionId, AbstractionType.NNAR, appRead.getRegister());
        process.registerAbstraction(new NNAtomicRegister(nnarAbstractionId, process));

        Protocol.NnarRead nnarRead = Protocol.NnarRead
                .newBuilder()
                .build();

        Protocol.Message nnarReadMessage = Protocol.Message
                .newBuilder()
                .setType(Protocol.Message.Type.NNAR_READ)
                .setNnarRead(nnarRead)
                .setFromAbstractionId(this.abstractionId)
                .setToAbstractionId(nnarAbstractionId)
                .setSystemId(process.getSystemId())
                .build();

        process.addMessageToQueue(nnarReadMessage);
    }

    private void handleAppWrite(Protocol.AppWrite appWrite) {
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
                .setSystemId(process.getSystemId())
                .build();

        process.addMessageToQueue(nnarWriteMessage);
    }


    private void handleNnarReadReturn(Protocol.NnarReadReturn nnarReadReturn, String fromAbstractionId) {
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
                .setSystemId(process.getSystemId())
                .build();

        triggerPlSend(appReadReturnMessage);
    }

    private void handleNnarWriteReturn(Protocol.NnarWriteReturn nnarWriteReturn, String fromAbstractionId) {
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
                .setSystemId(process.getSystemId())
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
                .setSystemId(process.getSystemId())
                .build();

        process.addMessageToQueue(plSendMessage);
    }
}
