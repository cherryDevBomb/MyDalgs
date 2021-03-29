package com.ubbcluj.amcds.myDalgs;

import com.ubbcluj.amcds.myDalgs.algorithms.Abstraction;
import com.ubbcluj.amcds.myDalgs.algorithms.PerfectLink;
import com.ubbcluj.amcds.myDalgs.communication.Protocol;
import com.ubbcluj.amcds.myDalgs.globals.HubInfo;
import com.ubbcluj.amcds.myDalgs.globals.ProcessConstants;
import com.ubbcluj.amcds.myDalgs.network.MessageConverter;
import com.ubbcluj.amcds.myDalgs.network.MessageReceiver;
import com.ubbcluj.amcds.myDalgs.network.MessageSender;
import com.ubbcluj.amcds.myDalgs.util.IncomingNetworkMessageWrapper;

import java.util.*;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.stream.Collectors;

public class Process implements Runnable, Observer {

    private Protocol.ProcessId process;

    private final Queue<Protocol.Message> messageQueue;
    private Map<Integer, Protocol.ProcessId> processes;
    private final List<Abstraction> abstractions;

    public Process(Protocol.ProcessId process) {
        this.process = process;
        this.messageQueue = new ConcurrentLinkedQueue<>();
        this.abstractions = getAbstractions();
    }

    private List<Abstraction> getAbstractions() {
        return Arrays.asList(new PerfectLink());
    }

    @Override
    public void run() {
        System.out.println("Running process " + process.getOwner() + "-" + process.getIndex());

        // start event loop
        Runnable eventLoop = () -> {
            while (true) {
                messageQueue.forEach(message -> {
                    abstractions.forEach(abstraction -> {
                        if (abstraction.canHandle(message)) {
                            System.out.println("Handled " + message.getType());
                            abstraction.handle(message);
                            messageQueue.remove(message);
                        }
                    });
                });
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
     * This method handles an incoming Message.
     * Triggered by calling the notifyObservers() method from the MessageReceiver or from an abstraction handler.
     * If message is of type PROC_INITIALIZE_SYSTEM, it is handled by the process.
     * If message is another type of message coming from the network, a PlDeliver message is created and added to the queue.
     * Otherwise, the message is directly added to the queue.
     *
     * @param o   source of the message
     * @param arg incoming message
     */
    @Override
    public void update(Observable o, Object arg) {
        if (arg instanceof IncomingNetworkMessageWrapper) {
            IncomingNetworkMessageWrapper messageWrapper = (IncomingNetworkMessageWrapper) arg;
            Protocol.Message innerMessage = messageWrapper.getMessage();
            if (Protocol.Message.Type.PROC_INITIALIZE_SYSTEM.equals(innerMessage.getType())) {
                handleProcInitializeSystem(innerMessage);
            } else {
                Protocol.ProcessId sender = processes.get(messageWrapper.getSenderPort());
                Protocol.Message convertedMessage = MessageConverter.createPlDeliverMessage(innerMessage, sender, messageWrapper.getToAbstractionId());
                messageQueue.add(convertedMessage);
            }
        } else if (arg instanceof Protocol.Message) {
            Protocol.Message message = (Protocol.Message) arg;
            messageQueue.add(message);
        }
    }

    private void handleProcInitializeSystem(Protocol.Message message) {
        Protocol.ProcInitializeSystem procInitializeSystem = message.getProcInitializeSystem();
        this.processes = procInitializeSystem.getProcessesList()
                .stream()
                .collect(Collectors.toMap(Protocol.ProcessId::getPort, processId -> processId));
    }
}
