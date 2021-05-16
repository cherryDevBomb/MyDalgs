package com.ubbcluj.amcds.myDalgs.algorithms;

import com.ubbcluj.amcds.myDalgs.Process;
import com.ubbcluj.amcds.myDalgs.communication.Protocol;
import com.ubbcluj.amcds.myDalgs.model.AbstractionType;
import com.ubbcluj.amcds.myDalgs.util.AbstractionIdUtil;

import java.util.*;
import java.util.concurrent.CopyOnWriteArraySet;

public class EventuallyPerfectFailureDetector extends Abstraction {

    private Set<Protocol.ProcessId> alive;
    private Set<Protocol.ProcessId> suspected;
    private int delay;

    private static final int DELTA = 100;

    public EventuallyPerfectFailureDetector(String abstractionId, Process process) {
        super(abstractionId, process);
        alive = new CopyOnWriteArraySet<>(process.getProcesses());
        suspected = new CopyOnWriteArraySet<>();
        delay = DELTA;
        startTimer(delay);

        process.registerAbstraction(new PerfectLink(AbstractionIdUtil.getChildAbstractionId(abstractionId, AbstractionType.PL), process));
    }

    @Override
    public boolean handle(Protocol.Message message) {
        switch (message.getType()) {
            case EPFD_TIMEOUT:
                handleEpfdTimeout();
                return true;
            case PL_DELIVER:
                Protocol.PlDeliver plDeliver = message.getPlDeliver();
                switch (plDeliver.getMessage().getType()) {
                    case EPFD_INTERNAL_HEARTBEAT_REQUEST:
                        handleHeartbeatRequest(plDeliver.getSender());
                        return true;
                    case EPFD_INTERNAL_HEARTBEAT_REPLY:
                        handleHeartbeatReply(plDeliver.getSender());
                        return true;
                    default:
                        return false;
                }
            default:
                return false;
        }
    }

    private void handleEpfdTimeout() {
        Set<Protocol.ProcessId> aliveSuspectIntersection = new CopyOnWriteArraySet<>(alive); // TODO check this
        aliveSuspectIntersection.retainAll(suspected);
        if (!aliveSuspectIntersection.isEmpty()) {
            delay += DELTA;
        }

        process.getProcesses().forEach(p -> {
            if (!alive.contains(p) && !suspected.contains(p)) {
                suspected.add(p);
                triggerSuspect(p);
            } else if (alive.contains(p) && suspected.contains(p)) {
                suspected.remove(p);
                triggerRestore(p);
            }
            triggerPlSendHeartbeatRequest(p);
        });

        alive.clear();
        startTimer(delay);
    }

    private void handleHeartbeatRequest(Protocol.ProcessId sender) {
        Protocol.EpfdInternalHeartbeatReply epfdHeartbeatReply = Protocol.EpfdInternalHeartbeatReply
                .newBuilder()
                .build();

        Protocol.Message heartbeatReplyMessage = Protocol.Message
                .newBuilder()
                .setType(Protocol.Message.Type.EPFD_INTERNAL_HEARTBEAT_REPLY)
                .setEpfdInternalHeartbeatReply(epfdHeartbeatReply)
                .setFromAbstractionId(this.abstractionId)
                .setToAbstractionId(this.abstractionId)
                .setSystemId(process.getSystemId())
                .build();

        Protocol.PlSend plSend = Protocol.PlSend
                .newBuilder()
                .setDestination(sender)
                .setMessage(heartbeatReplyMessage)
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

    private void handleHeartbeatReply(Protocol.ProcessId sender) {
        alive.add(sender);
    }

    private void triggerSuspect(Protocol.ProcessId p) {
        Protocol.EpfdSuspect epfdSuspect = Protocol.EpfdSuspect
                .newBuilder()
                .setProcess(p)
                .build();

        Protocol.Message suspectMessage = Protocol.Message
                .newBuilder()
                .setType(Protocol.Message.Type.EPFD_SUSPECT)
                .setEpfdSuspect(epfdSuspect)
                .setFromAbstractionId(this.abstractionId)
                .setToAbstractionId(AbstractionIdUtil.getParentAbstractionId(this.abstractionId)) //TODO WTF should be here?
                .setSystemId(process.getSystemId())
                .build();

        process.addMessageToQueue(suspectMessage);
    }

    private void triggerRestore(Protocol.ProcessId p) {
        Protocol.EpfdRestore epfdRestore = Protocol.EpfdRestore
                .newBuilder()
                .setProcess(p)
                .build();

        Protocol.Message restoreMessage = Protocol.Message
                .newBuilder()
                .setType(Protocol.Message.Type.EPFD_RESTORE)
                .setEpfdRestore(epfdRestore)
                .setFromAbstractionId(this.abstractionId)
                .setToAbstractionId(AbstractionIdUtil.getParentAbstractionId(this.abstractionId)) //TODO WTF should be here?
                .setSystemId(process.getSystemId())
                .build();

        process.addMessageToQueue(restoreMessage);
    }

    private void triggerPlSendHeartbeatRequest(Protocol.ProcessId p) {
        Protocol.EpfdInternalHeartbeatRequest epfdHeartbeatRequest = Protocol.EpfdInternalHeartbeatRequest
                .newBuilder()
                .build();

        Protocol.Message heartbeatRequestMessage = Protocol.Message
                .newBuilder()
                .setType(Protocol.Message.Type.EPFD_INTERNAL_HEARTBEAT_REQUEST)
                .setEpfdInternalHeartbeatRequest(epfdHeartbeatRequest)
                .setFromAbstractionId(this.abstractionId)
                .setToAbstractionId(this.abstractionId)
                .setSystemId(process.getSystemId())
                .build();

        Protocol.PlSend plSend = Protocol.PlSend
                .newBuilder()
                .setDestination(p)
                .setMessage(heartbeatRequestMessage)
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

    private void startTimer(int delay) {
        Protocol.EpfdTimeout epfdTimeout = Protocol.EpfdTimeout
                .newBuilder()
                .build();

        Protocol.Message timeoutMessage = Protocol.Message
                .newBuilder()
                .setType(Protocol.Message.Type.EPFD_TIMEOUT)
                .setEpfdTimeout(epfdTimeout)
                .setFromAbstractionId(this.abstractionId)
                .setToAbstractionId(this.abstractionId)
                .setSystemId(process.getSystemId())
                .build();

        Timer timer = new Timer();
        timer.schedule(new TimerTask() {
            @Override
            public void run() {
                process.addMessageToQueue(timeoutMessage);
            }
        }, delay);
    }
}
