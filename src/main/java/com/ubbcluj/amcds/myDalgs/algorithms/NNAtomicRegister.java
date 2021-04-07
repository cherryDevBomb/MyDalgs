package com.ubbcluj.amcds.myDalgs.algorithms;

import com.ubbcluj.amcds.myDalgs.Process;
import com.ubbcluj.amcds.myDalgs.communication.Protocol;
import com.ubbcluj.amcds.myDalgs.model.AbstractionType;
import com.ubbcluj.amcds.myDalgs.model.NNARValue;
import com.ubbcluj.amcds.myDalgs.util.AbstractionIdUtil;
import com.ubbcluj.amcds.myDalgs.util.ValueUtil;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class NNAtomicRegister extends Abstraction {

    private NNARValue nnarValue;
    private int acks;
    private Protocol.Value writeVal;
    private int readId;
    private Map<String, NNARValue> readList;
    private Protocol.Value readVal;
    private boolean isReading;

    public NNAtomicRegister(String abstractionId, Process process) {
        super(abstractionId, process);
        nnarValue = new NNARValue();
        acks = 0;
        writeVal = ValueUtil.buildUndefinedValue();
        readId = 0;
        readList = new ConcurrentHashMap<>();
        readVal = ValueUtil.buildUndefinedValue();
        isReading = false;

        process.registerAbstraction(new BestEffortBroadcast(AbstractionIdUtil.getChildAbstractionId(abstractionId, AbstractionType.BEB), process));
        process.registerAbstraction(new PerfectLink(AbstractionIdUtil.getChildAbstractionId(abstractionId, AbstractionType.PL), process));
    }

    @Override
    public boolean handle(Protocol.Message message) {
        switch (message.getType()) {
            case NNAR_READ:
                handleNnarRead(message.getSystemId());
                return true;
            case NNAR_WRITE:
                handleNnarWrite(message.getNnarWrite(), message.getSystemId());
                return true;
            case BEB_DELIVER:
                Protocol.BebDeliver bebDeliver = message.getBebDeliver();
                switch (bebDeliver.getMessage().getType()) {
                    case NNAR_INTERNAL_READ:
                        Protocol.NnarInternalRead nnarInternalRead = bebDeliver.getMessage().getNnarInternalRead();
                        handleBebDeliverInternalRead(bebDeliver.getSender(), nnarInternalRead.getReadId(), bebDeliver.getMessage().getSystemId());
                        return true;
                    case NNAR_INTERNAL_WRITE:
                        Protocol.NnarInternalWrite nnarInternalWrite = bebDeliver.getMessage().getNnarInternalWrite();
                        NNARValue value = new NNARValue(nnarInternalWrite.getTimestamp(), nnarInternalWrite.getWriterRank(), nnarInternalWrite.getValue());
                        handleBebDeliverInternalWrite(bebDeliver.getSender(), nnarInternalWrite.getReadId(), value, bebDeliver.getMessage().getSystemId());
                        return true;
                    default:
                        return false;
                }
            case PL_DELIVER:
                Protocol.PlDeliver plDeliver = message.getPlDeliver();
                switch (plDeliver.getMessage().getType()) {
                    case NNAR_INTERNAL_VALUE:
                        Protocol.NnarInternalValue nnarInternalValue = plDeliver.getMessage().getNnarInternalValue();
                        if (nnarInternalValue.getReadId() == this.readId) {
                            NNARValue value = new NNARValue(nnarInternalValue.getTimestamp(), nnarInternalValue.getWriterRank(), nnarInternalValue.getValue());
                            triggerPlDeliverValue(plDeliver.getSender(), nnarInternalValue.getReadId(), value, plDeliver.getMessage().getSystemId());
                            return true;
                        }
                        return false;
                    case NNAR_INTERNAL_ACK:
                        Protocol.NnarInternalAck nnarInternalAck = plDeliver.getMessage().getNnarInternalAck();
                        if (nnarInternalAck.getReadId() == this.readId) {
                            triggerPlDeliverAck(plDeliver.getSender(), nnarInternalAck.getReadId(), plDeliver.getMessage().getSystemId());
                            return true;
                        }
                        return false;
                    default:
                        return false;
                }
            default:
                return false;
        }
    }

    private void handleNnarRead(String systemId) {
        readId++;
        acks = 0;
        readList = new ConcurrentHashMap<>();
        isReading = true;

        triggerBebBroadcast(systemId);
    }

    private void handleNnarWrite(Protocol.NnarWrite nnarWrite, String systemId) {
        readId++;
        writeVal = Protocol.Value
                .newBuilder()
                .setV(nnarWrite.getValue().getV())
                .setDefined(true)
                .build();
        acks = 0;
        readList = new ConcurrentHashMap<>();

        triggerBebBroadcast(systemId);
    }

    private void handleBebDeliverInternalRead(Protocol.ProcessId sender, int incomingReadId, String systemId) {
    }

    private void handleBebDeliverInternalWrite(Protocol.ProcessId sender, int incomingReadId, NNARValue incomingVal, String systemId) {
        if (incomingVal.getTimestamp() > nnarValue.getTimestamp() || (incomingVal.getTimestamp() == nnarValue.getTimestamp() && incomingVal.getWriterRank() > nnarValue.getWriterRank())) {
            nnarValue = incomingVal;
        }

        Protocol.NnarInternalAck nnarInternalAck = Protocol.NnarInternalAck
                .newBuilder()
                .setReadId(incomingReadId)
                .build();

        Protocol.Message nnarInternalAckMessage = Protocol.Message
                .newBuilder()
                .setType(Protocol.Message.Type.NNAR_INTERNAL_ACK)
                .setNnarInternalAck(nnarInternalAck)
                .setFromAbstractionId(this.abstractionId)
                .setToAbstractionId(this.abstractionId)
                .setSystemId(systemId)
                .build();

        Protocol.PlSend plSend = Protocol.PlSend
                .newBuilder()
                .setDestination(sender)
                .setMessage(nnarInternalAckMessage)
                .build();

        Protocol.Message plSendMessage = Protocol.Message
                .newBuilder()
                .setType(Protocol.Message.Type.PL_SEND)
                .setPlSend(plSend)
                .setFromAbstractionId(this.abstractionId)
                .setToAbstractionId(AbstractionIdUtil.getChildAbstractionId(this.abstractionId, AbstractionType.PL))
                .setSystemId(systemId)
                .build();

        process.addMessageToQueue(plSendMessage);
    }

    private void triggerPlDeliverValue(Protocol.ProcessId sender, int incomingReadId, NNARValue value, String systemId) {
    }

    private void triggerPlDeliverAck(Protocol.ProcessId sender, int incomingReadId, String systemId) {
        acks++;
        if (acks > (process.getProcesses().size() / 2)) {
            acks = 0;
            if (isReading) {
                isReading = false;
                Protocol.NnarReadReturn nnarReadReturn = Protocol.NnarReadReturn
                        .newBuilder()
                        .setValue(readVal)
                        .build();

                Protocol.Message nnarReadReturnMessage = Protocol.Message
                        .newBuilder()
                        .setType(Protocol.Message.Type.NNAR_READ_RETURN)
                        .setNnarReadReturn(nnarReadReturn)
                        .setFromAbstractionId(this.abstractionId)
                        .setToAbstractionId(AbstractionIdUtil.getParentAbstractionId(this.abstractionId))
                        .setSystemId(systemId)
                        .build();

                process.addMessageToQueue(nnarReadReturnMessage);
            } else {
                Protocol.NnarWriteReturn nnarWriteReturn = Protocol.NnarWriteReturn
                        .newBuilder()
                        .build();

                Protocol.Message nnarWriteReturnMessage = Protocol.Message
                        .newBuilder()
                        .setType(Protocol.Message.Type.NNAR_WRITE_RETURN)
                        .setNnarWriteReturn(nnarWriteReturn)
                        .setFromAbstractionId(this.abstractionId)
                        .setToAbstractionId(AbstractionIdUtil.getParentAbstractionId(this.abstractionId))
                        .setSystemId(systemId)
                        .build();

                process.addMessageToQueue(nnarWriteReturnMessage);
            }
        }
    }

    private void triggerBebBroadcast(String systemId) {
        Protocol.NnarInternalRead nnarInternalRead = Protocol.NnarInternalRead
                .newBuilder()
                .setReadId(readId)
                .build();

        Protocol.Message nnarInternalReadMessage = Protocol.Message
                .newBuilder()
                .setType(Protocol.Message.Type.NNAR_INTERNAL_READ)
                .setNnarInternalRead(nnarInternalRead)
                .setFromAbstractionId(this.abstractionId)
                .setToAbstractionId(this.abstractionId)
                .setSystemId(systemId)
                .build();

        Protocol.BebBroadcast bebBroadcast = Protocol.BebBroadcast
                .newBuilder()
                .setMessage(nnarInternalReadMessage)
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
}
