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
                handleNnarRead();
                return true;
            case NNAR_WRITE:
                handleNnarWrite(message.getNnarWrite(), message.getSystemId());
                return true;
            case BEB_DELIVER:
                Protocol.BebDeliver bebDeliver = message.getBebDeliver();
                switch (bebDeliver.getMessage().getType()) {
                    case NNAR_INTERNAL_READ:
                        Protocol.NnarInternalRead nnarInternalRead = bebDeliver.getMessage().getNnarInternalRead();
                        handleBebDeliverInternalRead(bebDeliver.getSender(), nnarInternalRead.getReadId());
                        return true;
                    case NNAR_INTERNAL_WRITE:
                        Protocol.NnarInternalWrite nnarInternalWrite = bebDeliver.getMessage().getNnarInternalWrite();
                        NNARValue value = new NNARValue(nnarInternalWrite.getTimestamp(), nnarInternalWrite.getWriterRank(), nnarInternalWrite.getValue());
                        handleBebDeliverInternalWrite(bebDeliver.getSender(), nnarInternalWrite.getReadId(), value);
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
                            triggerPlDeliverValue(plDeliver.getSender(), nnarInternalValue.getReadId(), value);
                            return true;
                        }
                        return false;
                    case NNAR_INTERNAL_ACK:
                        Protocol.NnarInternalAck nnarInternalAck = plDeliver.getMessage().getNnarInternalAck();
                        if (nnarInternalAck.getReadId() == this.readId) {
                            triggerPlDeliverAck(plDeliver.getSender(), nnarInternalAck.getReadId());
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

    private void handleNnarRead() {
    }

    private void handleNnarWrite(Protocol.NnarWrite nnarWrite, String systemId) {
    }

    private void handleBebDeliverInternalRead(Protocol.ProcessId sender, int readId) {
    }

    private void handleBebDeliverInternalWrite(Protocol.ProcessId sender, int readId, NNARValue value) {
    }

    private void triggerPlDeliverValue(Protocol.ProcessId sender, int readId, NNARValue value) {
    }

    private void triggerPlDeliverAck(Protocol.ProcessId sender, int readId) {
    }
}
