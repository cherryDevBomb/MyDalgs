package com.ubbcluj.amcds.myDalgs.algorithms;

import com.ubbcluj.amcds.myDalgs.Process;
import com.ubbcluj.amcds.myDalgs.communication.Protocol;
import com.ubbcluj.amcds.myDalgs.model.AbstractionType;
import com.ubbcluj.amcds.myDalgs.model.EpState;
import com.ubbcluj.amcds.myDalgs.util.AbstractionIdUtil;
import com.ubbcluj.amcds.myDalgs.util.DalgsUtil;

public class UniformConsensus extends Abstraction {

    private Protocol.Value val;
    private boolean proposed;
    private boolean decided;
    private int ets;
    private Protocol.ProcessId leader;
    private int newts;
    private Protocol.ProcessId newLeader;

    public UniformConsensus(String abstractionId, Process process) {
        super(abstractionId, process);

        val = DalgsUtil.buildUndefinedValue();
        proposed = false;
        decided = false;

        Protocol.ProcessId leader0 = DalgsUtil.getMaxRankedProcess(process.getProcesses());
        ets = 0;
        leader = leader0;
        newts = 0;
        newLeader = null;

        process.registerAbstraction(new EpochChange(AbstractionIdUtil.getChildAbstractionId(abstractionId, AbstractionType.EC), process));
        process.registerAbstraction(new EpochConsensus(AbstractionIdUtil.getNamedAbstractionId(abstractionId, AbstractionType.EP, "0"), process,
                0, leader0, new EpState(0, DalgsUtil.buildUndefinedValue())));
    }

    @Override
    public boolean handle(Protocol.Message message) {
        switch (message.getType()) {
            case UC_PROPOSE:
                val = message.getUcPropose().getValue();
                performCheck();
                return true;
            case EC_START_EPOCH:
                newts = message.getEcStartEpoch().getNewTimestamp();
                newLeader = message.getEcStartEpoch().getNewLeader();
                triggerEpEtsAbort();
            case EP_ABORTED:
                if (message.getEpAborted().getEts() == ets) {
                    ets = newts;
                    leader = newLeader;
                    proposed = false;
                    process.registerAbstraction(new EpochConsensus(AbstractionIdUtil.getNamedAbstractionId(abstractionId, AbstractionType.EP, Integer.toString(ets)), process,
                            ets, leader, new EpState(message.getEpAborted().getValueTimestamp(), message.getEpAborted().getValue())));
                    performCheck();
                    return true;
                }
                return false;
            case EP_DECIDE:
                if (message.getEpDecide().getEts() == ets) {
                    if (!decided) {
                        decided = true;
                        triggerUcDecide(message.getEpDecide().getValue());
                    }
                }
            default:
                return false;
        }
    }

    private void triggerEpEtsAbort() {
        Protocol.EpAbort epAbort = Protocol.EpAbort
                .newBuilder()
                .build();

        Protocol.Message epAbortMessage = Protocol.Message
                .newBuilder()
                .setType(Protocol.Message.Type.EP_ABORT)
                .setEpAbort(epAbort)
                .setFromAbstractionId(this.abstractionId)
                .setToAbstractionId(AbstractionIdUtil.getNamedAbstractionId(this.abstractionId, AbstractionType.EP, Integer.toString(ets)))
                .setSystemId(process.getSystemId())
                .build();

        process.addMessageToQueue(epAbortMessage);
    }

    private void performCheck() {
        if (leader.equals(process.getProcess()) && val.getDefined() && !proposed) {
            proposed = true;

            Protocol.EpPropose epPropose = Protocol.EpPropose
                    .newBuilder()
                    .setValue(val)
                    .build();

            Protocol.Message epProposeMessage = Protocol.Message
                    .newBuilder()
                    .setType(Protocol.Message.Type.EP_PROPOSE)
                    .setEpPropose(epPropose)
                    .setFromAbstractionId(this.abstractionId)
                    .setToAbstractionId(AbstractionIdUtil.getNamedAbstractionId(this.abstractionId, AbstractionType.EP, Integer.toString(ets)))
                    .setSystemId(process.getSystemId())
                    .build();

            process.addMessageToQueue(epProposeMessage);
        }
    }

    private void triggerUcDecide(Protocol.Value value) {
        Protocol.UcDecide ucDecide = Protocol.UcDecide
                .newBuilder()
                .setValue(value)
                .build();

        Protocol.Message ucDecideMessage = Protocol.Message
                .newBuilder()
                .setType(Protocol.Message.Type.UC_DECIDE)
                .setUcDecide(ucDecide)
                .setFromAbstractionId(this.abstractionId)
                .setToAbstractionId(AbstractionIdUtil.getParentAbstractionId(this.abstractionId))
                .setSystemId(process.getSystemId())
                .build();

        process.addMessageToQueue(ucDecideMessage);
    }
}
