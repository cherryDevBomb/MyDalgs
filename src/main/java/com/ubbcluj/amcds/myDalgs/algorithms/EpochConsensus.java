package com.ubbcluj.amcds.myDalgs.algorithms;

import com.ubbcluj.amcds.myDalgs.Process;
import com.ubbcluj.amcds.myDalgs.communication.Protocol;
import com.ubbcluj.amcds.myDalgs.model.AbstractionType;
import com.ubbcluj.amcds.myDalgs.model.EpState;
import com.ubbcluj.amcds.myDalgs.util.AbstractionIdUtil;
import com.ubbcluj.amcds.myDalgs.util.DalgsUtil;

import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;

public class EpochConsensus extends Abstraction {

    private int ets;
    private Protocol.ProcessId leader;

    private EpState state;
    private Protocol.Value tmpVal;
    private Map<Protocol.ProcessId, EpState> states;
    private int accepted;
    private boolean halted;

    public EpochConsensus(String abstractionId, Process process, int ets, Protocol.ProcessId leader, EpState state) {
        super(abstractionId, process);
        this.ets = ets;
        this.leader = leader;

        this.state = state;
        this.tmpVal = DalgsUtil.buildUndefinedValue();
        this.states = new HashMap<>();
        this.accepted = 0;
        this.halted = false;

        process.registerAbstraction(new BestEffortBroadcast(AbstractionIdUtil.getChildAbstractionId(abstractionId, AbstractionType.BEB), process));
        process.registerAbstraction(new PerfectLink(AbstractionIdUtil.getChildAbstractionId(abstractionId, AbstractionType.PL), process));
    }

    @Override
    public boolean handle(Protocol.Message message) {
        if (halted) {
            return false;
        }

        switch (message.getType()) {
            case EP_PROPOSE:
                tmpVal = message.getEpPropose().getValue();
                triggerBebBroadcastEpInternalRead();
                return true;
            case BEB_DELIVER:
                Protocol.BebDeliver bebDeliver = message.getBebDeliver();
                switch (bebDeliver.getMessage().getType()) {
                    case EP_INTERNAL_READ:
                        triggerPlSendEpState(bebDeliver.getSender());
                        return true;
                    case EP_INTERNAL_WRITE:
                        state = new EpState(ets, bebDeliver.getMessage().getEpInternalWrite().getValue());
                        triggerPlSendEpAccept(bebDeliver.getSender());
                        return true;
                    case EP_INTERNAL_DECIDED:
                        triggerEpDecide(bebDeliver.getMessage().getEpInternalDecided().getValue());
                        return true;
                    default:
                        return false;
                }
            case PL_DELIVER:
                Protocol.PlDeliver plDeliver = message.getPlDeliver();
                switch (plDeliver.getMessage().getType()) {
                    case EP_INTERNAL_STATE:
                        Protocol.EpInternalState deliveredState = plDeliver.getMessage().getEpInternalState();
                        states.put(plDeliver.getSender(), new EpState(deliveredState.getValueTimestamp(), deliveredState.getValue()));
                        performStatesCheck();
                        return true;
                    case EP_INTERNAL_ACCEPT:
                        accepted++;
                        performAcceptedCheck();
                        return true;
                    default:
                        return false;
                }
            case EP_ABORT:
                triggerEpAborted();
                halted = true;
                return true;
            default:
                return false;
        }
    }

    private void triggerBebBroadcastEpInternalRead() {
        Protocol.EpInternalRead epRead = Protocol.EpInternalRead
                .newBuilder()
                .build();

        Protocol.Message epReadMessage = Protocol.Message
                .newBuilder()
                .setType(Protocol.Message.Type.EP_INTERNAL_READ)
                .setEpInternalRead(epRead)
                .setFromAbstractionId(this.abstractionId)
                .setToAbstractionId(this.abstractionId)
                .setSystemId(process.getSystemId())
                .build();

        triggerBebBroadcast(epReadMessage);
    }

    private void triggerPlSendEpState(Protocol.ProcessId sender) {
        Protocol.EpInternalState epState = Protocol.EpInternalState
                .newBuilder()
                .setValueTimestamp(state.getValTimestamp())
                .setValue(state.getVal())
                .build();

        Protocol.Message epStateMessage = Protocol.Message
                .newBuilder()
                .setType(Protocol.Message.Type.EP_INTERNAL_STATE)
                .setEpInternalState(epState)
                .setFromAbstractionId(this.abstractionId)
                .setToAbstractionId(this.abstractionId)
                .setSystemId(process.getSystemId())
                .build();

        triggerPlSend(epStateMessage, sender);
    }

    private void triggerPlSendEpAccept(Protocol.ProcessId sender) {
        Protocol.EpInternalAccept epAccept = Protocol.EpInternalAccept
                .newBuilder()
                .build();

        Protocol.Message epAcceptMessage = Protocol.Message
                .newBuilder()
                .setType(Protocol.Message.Type.EP_INTERNAL_ACCEPT)
                .setEpInternalAccept(epAccept)
                .setFromAbstractionId(this.abstractionId)
                .setToAbstractionId(this.abstractionId)
                .setSystemId(process.getSystemId())
                .build();

        triggerPlSend(epAcceptMessage, sender);
    }

    private void performStatesCheck() {
        if (states.size() > process.getProcesses().size() / 2) {
            EpState highest = getHighestState();
            if (highest.getVal().getDefined()) {
                tmpVal = highest.getVal();
            }
            states.clear();

            Protocol.EpInternalWrite epWrite = Protocol.EpInternalWrite
                    .newBuilder()
                    .setValue(tmpVal)
                    .build();

            Protocol.Message epWriteMessage = Protocol.Message
                    .newBuilder()
                    .setType(Protocol.Message.Type.EP_INTERNAL_WRITE)
                    .setEpInternalWrite(epWrite)
                    .setFromAbstractionId(this.abstractionId)
                    .setToAbstractionId(this.abstractionId)
                    .setSystemId(process.getSystemId())
                    .build();

            triggerBebBroadcast(epWriteMessage);
        }
    }

    private void performAcceptedCheck() {
        if (accepted > process.getProcesses().size() / 2) {
            accepted = 0;

            Protocol.EpInternalDecided epDecided = Protocol.EpInternalDecided
                    .newBuilder()
                    .setValue(tmpVal)
                    .build();

            Protocol.Message epDecidedMessage = Protocol.Message
                    .newBuilder()
                    .setType(Protocol.Message.Type.EP_INTERNAL_DECIDED)
                    .setEpInternalDecided(epDecided)
                    .setFromAbstractionId(this.abstractionId)
                    .setToAbstractionId(this.abstractionId)
                    .setSystemId(process.getSystemId())
                    .build();

            triggerBebBroadcast(epDecidedMessage);
        }
    }

    private void triggerEpDecide(Protocol.Value value) {
        Protocol.EpDecide epDecide = Protocol.EpDecide
                .newBuilder()
                .setEts(ets)
                .setValue(value)
                .build();

        Protocol.Message epDecideMessage = Protocol.Message
                .newBuilder()
                .setType(Protocol.Message.Type.EP_DECIDE)
                .setEpDecide(epDecide)
                .setFromAbstractionId(this.abstractionId)
                .setToAbstractionId(AbstractionIdUtil.getParentAbstractionId(this.abstractionId))
                .setSystemId(process.getSystemId())
                .build();

        process.addMessageToQueue(epDecideMessage);
    }

    private void triggerEpAborted() {
        Protocol.EpAborted epAborted = Protocol.EpAborted
                .newBuilder()
                .setEts(ets)
                .setValueTimestamp(state.getValTimestamp())
                .setValue(state.getVal())
                .build();

        Protocol.Message epAbortedMessage = Protocol.Message
                .newBuilder()
                .setType(Protocol.Message.Type.EP_ABORTED)
                .setEpAborted(epAborted)
                .setFromAbstractionId(this.abstractionId)
                .setToAbstractionId(AbstractionIdUtil.getParentAbstractionId(this.abstractionId))
                .setSystemId(process.getSystemId())
                .build();

        process.addMessageToQueue(epAbortedMessage);
    }

    private void triggerBebBroadcast(Protocol.Message message) {
        Protocol.BebBroadcast bebBroadcast = Protocol.BebBroadcast
                .newBuilder()
                .setMessage(message)
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

    private void triggerPlSend(Protocol.Message message, Protocol.ProcessId destination) {
        Protocol.PlSend plSend = Protocol.PlSend
                .newBuilder()
                .setDestination(destination)
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

    private EpState getHighestState() {
        return states.values().stream()
                .max(Comparator.comparing(EpState::getValTimestamp))
                .orElse(new EpState(0, DalgsUtil.buildUndefinedValue()));
    }
}
