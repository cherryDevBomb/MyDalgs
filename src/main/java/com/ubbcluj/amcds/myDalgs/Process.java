package com.ubbcluj.amcds.myDalgs;

import com.ubbcluj.amcds.myDalgs.communication.CommunicationProtocol;
import com.ubbcluj.amcds.myDalgs.globals.HubInfo;
import com.ubbcluj.amcds.myDalgs.globals.ProcessConstants;
import com.ubbcluj.amcds.myDalgs.network.MessageReceiver;
import com.ubbcluj.amcds.myDalgs.network.MessageSender;

import java.util.Queue;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentLinkedQueue;

public class Process implements Runnable {

    private CommunicationProtocol.ProcessId process;

    private final Queue<CommunicationProtocol.Message> messageQueue;
    //    private final List<Algorithm> algorithms;
    private Set<CommunicationProtocol.ProcessId> allProcesses;

    public Process(CommunicationProtocol.ProcessId process) {
        this.process = process;
        this.messageQueue = new ConcurrentLinkedQueue<>();
    }

    public void run() {
        System.out.println("Running process " + process.getOwner() + "-" + process.getIndex());

        // add app to algorithms? / init algorithms

        // start event loop
        Runnable eventLoop = () -> {
            while (true) {
                System.out.println("Running event loop in " + process.getIndex());
                break;
//                messageQueue.forEach(message -> {
//                    algorithms.forEach(algorithm -> {
//                        try {
//                            if (algorithm.handle(message)) {
//                                logMessageInfo(message, algorithm);
//                                messageQueue.remove(message);
//                            }
//                        } catch (IOException e) {
//                            e.printStackTrace();
//                        }
//                    });
//                });
            }
        };
        new Thread(eventLoop).start();

        new Thread(new MessageReceiver(process.getPort())).start();

        register();
    }

    private void register() {
        CommunicationProtocol.ProcRegistration procRegistration = CommunicationProtocol.ProcRegistration
                .newBuilder()
                .setOwner(process.getOwner())
                .setIndex(process.getIndex())
                .build();

        CommunicationProtocol.Message message = CommunicationProtocol.Message
                .newBuilder()
                .setType(CommunicationProtocol.Message.Type.PROC_REGISTRATION)
                .setProcRegistration(procRegistration)
                .setMessageUuid(UUID.randomUUID().toString())
//                .setFromAbstractionId("app") //TODO is this needed?
                .setToAbstractionId("app")
                .setSystemId(ProcessConstants.SYSTEM_ID)
                .build();

        MessageSender.send(message, process, HubInfo.HOST, HubInfo.PORT);
    }
}
