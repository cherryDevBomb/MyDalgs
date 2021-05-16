package com.ubbcluj.amcds.myDalgs.algorithms;

import com.ubbcluj.amcds.myDalgs.Process;
import com.ubbcluj.amcds.myDalgs.communication.Protocol;
import com.ubbcluj.amcds.myDalgs.model.AbstractionType;
import com.ubbcluj.amcds.myDalgs.util.AbstractionIdUtil;
import com.ubbcluj.amcds.myDalgs.util.DalgsUtil;

public class EpochChange extends Abstraction {

    private Protocol.ProcessId trusted;
    private int lastTimestamp;
    private int timestamp;

    public EpochChange(String abstractionId, Process process) {
        super(abstractionId, process);
        trusted = DalgsUtil.getMaxRankedProcess(process.getProcesses());
        lastTimestamp = 0;
        timestamp = process.getProcess().getRank();

        process.registerAbstraction(new EventualLeaderDetector(AbstractionIdUtil.getChildAbstractionId(abstractionId, AbstractionType.ELD), process));
        process.registerAbstraction(new BestEffortBroadcast(AbstractionIdUtil.getChildAbstractionId(abstractionId, AbstractionType.BEB), process));
        process.registerAbstraction(new PerfectLink(AbstractionIdUtil.getChildAbstractionId(abstractionId, AbstractionType.PL), process));
    }

    @Override
    public boolean handle(Protocol.Message message) {
        switch (message.getType()) {
            case ELD_TRUST:
                handleEldTrust(message.getEldTrust().getProcess());
                return true;
            case BEB_DELIVER:
                Protocol.BebDeliver bebDeliver = message.getBebDeliver();
                switch (bebDeliver.getMessage().getType()) {
                    case EC_INTERNAL_NEW_EPOCH:
                        handleBebDeliverNewEpoch(bebDeliver.getSender(), bebDeliver.getMessage().getEcInternalNewEpoch().getTimestamp());
                        return true;
                    default:
                        return false;
                }
            case PL_DELIVER:
                Protocol.PlDeliver plDeliver = message.getPlDeliver();
                switch (plDeliver.getMessage().getType()) {
                    case EC_INTERNAL_NACK:
                        handleNack();
                        return true;
                    default:
                        return false;
                }
            default:
                return false;
        }
    }

    private void handleEldTrust(Protocol.ProcessId p) {
        trusted = p;
        if (p.equals(process.getProcess())) {
            timestamp += process.getProcesses().size();
            triggerBebBroadcastNewEpoch();
        }
    }

    private void handleBebDeliverNewEpoch(Protocol.ProcessId sender, int newTimestamp) {
        if (sender.equals(trusted) && newTimestamp > lastTimestamp) {
            lastTimestamp = newTimestamp;
            Protocol.EcStartEpoch startEpoch = Protocol.EcStartEpoch
                    .newBuilder()
                    .setNewLeader(sender)
                    .setNewTimestamp(newTimestamp)
                    .build();

            Protocol.Message startEpochMessage = Protocol.Message
                    .newBuilder()
                    .setType(Protocol.Message.Type.EC_START_EPOCH)
                    .setEcStartEpoch(startEpoch)
                    .setFromAbstractionId(this.abstractionId)
                    .setToAbstractionId(AbstractionIdUtil.getParentAbstractionId(this.abstractionId))
                    .setSystemId(process.getSystemId())
                    .build();

            process.addMessageToQueue(startEpochMessage);
        } else {
            Protocol.EcInternalNack ecNack = Protocol.EcInternalNack
                    .newBuilder()
                    .build();

            Protocol.Message nackMessage = Protocol.Message
                    .newBuilder()
                    .setType(Protocol.Message.Type.EC_INTERNAL_NACK)
                    .setEcInternalNack(ecNack)
                    .setFromAbstractionId(this.abstractionId)
                    .setToAbstractionId(this.abstractionId)
                    .setSystemId(process.getSystemId())
                    .build();

            Protocol.PlSend plSend = Protocol.PlSend
                    .newBuilder()
                    .setDestination(sender)
                    .setMessage(nackMessage)
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

    private void handleNack() {
        if (trusted.equals(process.getProcess())) {
            timestamp += process.getProcesses().size();
            triggerBebBroadcastNewEpoch();
        }
    }

    private void triggerBebBroadcastNewEpoch() {
        Protocol.EcInternalNewEpoch newEpoch = Protocol.EcInternalNewEpoch
                .newBuilder()
                .setTimestamp(timestamp)
                .build();

        Protocol.Message newEpochMessage = Protocol.Message
                .newBuilder()
                .setType(Protocol.Message.Type.EC_INTERNAL_NEW_EPOCH)
                .setEcInternalNewEpoch(newEpoch)
                .setFromAbstractionId(this.abstractionId)
                .setToAbstractionId(this.abstractionId)
                .setSystemId(process.getSystemId())
                .build();

        Protocol.BebBroadcast bebBroadcast = Protocol.BebBroadcast
                .newBuilder()
                .setMessage(newEpochMessage)
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
}
