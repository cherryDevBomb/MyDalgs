package com.ubbcluj.amcds.myDalgs.algorithms;

import com.ubbcluj.amcds.myDalgs.Process;
import com.ubbcluj.amcds.myDalgs.communication.Protocol;
import com.ubbcluj.amcds.myDalgs.model.AbstractionType;
import com.ubbcluj.amcds.myDalgs.util.AbstractionIdUtil;
import com.ubbcluj.amcds.myDalgs.util.DalgsUtil;

import java.util.HashSet;
import java.util.Set;

public class EventualLeaderDetector extends Abstraction {

    private Set<Protocol.ProcessId> suspected;
    private Protocol.ProcessId leader;

    public EventualLeaderDetector(String abstractionId, Process process) {
        super(abstractionId, process);
        suspected = new HashSet<>();

        process.registerAbstraction(new EventuallyPerfectFailureDetector(AbstractionIdUtil.getChildAbstractionId(abstractionId, AbstractionType.EPFD), process));
    }

    @Override
    public boolean handle(Protocol.Message message) {
        switch (message.getType()) {
            case EPFD_SUSPECT:
                suspected.add(message.getEpfdSuspect().getProcess());
                performCheck();
                return true;
            case EPFD_RESTORE:
                suspected.remove(message.getEpfdSuspect().getProcess());
                performCheck();
            default:
                return false;
        }
    }

    private void performCheck() {
        Set<Protocol.ProcessId> notSuspected = new HashSet<>(process.getProcesses());
        notSuspected.removeAll(suspected);
        Protocol.ProcessId maxRankedProcess = DalgsUtil.getMaxRankedProcess(notSuspected);
        if (!leader.equals(maxRankedProcess)) {
            leader = maxRankedProcess;

            Protocol.EldTrust eldTrust = Protocol.EldTrust
                    .newBuilder()
                    .setProcess(leader)
                    .build();

            Protocol.Message trustMessage = Protocol.Message
                    .newBuilder()
                    .setType(Protocol.Message.Type.ELD_TRUST)
                    .setEldTrust(eldTrust)
                    .setFromAbstractionId(this.abstractionId)
                    .setToAbstractionId(AbstractionIdUtil.getParentAbstractionId(this.abstractionId)) //TODO WTF should be here?
                    .setSystemId(process.getSystemId())
                    .build();

            process.addMessageToQueue(trustMessage);
        }
    }
}
