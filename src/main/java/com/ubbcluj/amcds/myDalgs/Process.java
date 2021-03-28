package com.ubbcluj.amcds.myDalgs;

import com.ubbcluj.amcds.myDalgs.communication.Protocol;
import com.ubbcluj.amcds.myDalgs.globals.HubInfo;
import com.ubbcluj.amcds.myDalgs.globals.ProcessConstants;
import com.ubbcluj.amcds.myDalgs.network.MessageConverter;
import com.ubbcluj.amcds.myDalgs.network.MessageReceiver;
import com.ubbcluj.amcds.myDalgs.network.MessageSender;
import com.ubbcluj.amcds.myDalgs.util.IncomingMessageWrapper;

import java.util.*;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.stream.Collectors;

public class Process implements Runnable, Observer {

    private Protocol.ProcessId process;

    private final Queue<Protocol.Message> messageQueue;
    private Map<Integer, Protocol.ProcessId> processes;
    //    private final List<Algorithm> algorithms;

    public Process(Protocol.ProcessId process) {
        this.process = process;
        this.messageQueue = new ConcurrentLinkedQueue<>();
    }

    @Override
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
        Thread eventLoopThread = new Thread(eventLoop);
        eventLoopThread.start();

        MessageReceiver messageReceiver = new MessageReceiver(process.getPort());
        messageReceiver.addObserver(this);
        Thread messageReceiverThread = new Thread(messageReceiver);
        messageReceiverThread.start();

        register();

        try {
            messageReceiverThread.join();
            eventLoopThread.join();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    private void register() {
        Protocol.ProcRegistration procRegistration = Protocol.ProcRegistration
                .newBuilder()
                .setOwner(process.getOwner())
                .setIndex(process.getIndex())
                .build();

        Protocol.Message message = Protocol.Message
                .newBuilder()
                .setType(Protocol.Message.Type.PROC_REGISTRATION)
                .setProcRegistration(procRegistration)
                .setMessageUuid(UUID.randomUUID().toString())
                .setToAbstractionId("app")
                .setSystemId(ProcessConstants.SYSTEM_ID)
                .build();

        MessageSender.send(message, process, HubInfo.HOST, HubInfo.PORT);
    }

    /**
     * This method handles an incoming NetworkMessage.
     * If message is of type PROC_INITIALIZE_SYSTEM, it is handled by the process. Otherwise, the message is added to the queue.
     * Triggered by calling the notifyObservers() method from the MessageReceiver after a message is received.
     *
     * @param o   MessageReceiver
     * @param arg Protocol.Message
     */
    @Override
    public void update(Observable o, Object arg) {
        if (arg instanceof IncomingMessageWrapper) {
            IncomingMessageWrapper messageWrapper = (IncomingMessageWrapper) arg;
            Protocol.Message innerMessage = messageWrapper.getMessage();
            if (Protocol.Message.Type.PROC_INITIALIZE_SYSTEM.equals(innerMessage.getType())) {
                handleProcInitializeSystem(innerMessage);
            } else {
                Protocol.ProcessId sender = processes.get(messageWrapper.getSenderPort());
                Protocol.Message convertedMessage = MessageConverter.createPlDeliverMessage(innerMessage, sender, messageWrapper.getToAbstractionId());
                messageQueue.add(convertedMessage);
            }
        }
    }

    private void handleProcInitializeSystem(Protocol.Message message) {
        Protocol.ProcInitializeSystem procInitializeSystem = message.getProcInitializeSystem();
        this.processes = procInitializeSystem.getProcessesList()
                .stream()
                .collect(Collectors.toMap(Protocol.ProcessId::getPort, processId -> processId));
    }
}
