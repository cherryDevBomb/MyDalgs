package com.ubbcluj.amcds.myDalgs;

import com.ubbcluj.amcds.myDalgs.algorithms.Abstraction;
import com.ubbcluj.amcds.myDalgs.algorithms.Application;
import com.ubbcluj.amcds.myDalgs.communication.Protocol;
import com.ubbcluj.amcds.myDalgs.globals.AbstractionType;
import com.ubbcluj.amcds.myDalgs.globals.HubInfo;
import com.ubbcluj.amcds.myDalgs.globals.ProcessConstants;
import com.ubbcluj.amcds.myDalgs.network.MessageReceiver;
import com.ubbcluj.amcds.myDalgs.network.MessageSender;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;

public class Process implements Runnable, Observer {

    private Protocol.ProcessId process;

    private final Queue<Protocol.Message> messageQueue;
    private List<Protocol.ProcessId> processes;
    private final Map<String, Abstraction> abstractions;

    public Process(Protocol.ProcessId process) {
        this.process = process;
        this.messageQueue = new ConcurrentLinkedQueue<>();
        this.abstractions = new ConcurrentHashMap<>();
    }

    public Protocol.ProcessId getProcess() {
        return process;
    }

    public Optional<Protocol.ProcessId> getProcessByHostAndPort(String host, int port) {
        return processes.stream()
                .filter(p -> host.equals(p.getHost()) && p.getPort() == port)
                .findFirst();
    }

    public List<Protocol.ProcessId> getProcesses() {
        return processes;
    }

    @Override
    public void run() {
        System.out.println("Running process " + process.getOwner() + "-" + process.getIndex());

        //register abstractions
//        registerAbstraction(new PerfectLink(AbstractionType.PL.getId(), this)); //TODO is this needed?
//        registerAbstraction(new BestEffortBroadcast(AbstractionType.BEB.getId(), this)); //TODO is this needed?
        registerAbstraction(new Application(AbstractionType.APP.getId(), this));

        // start event loop
        Runnable eventLoop = () -> {
            while (true) {
                messageQueue.forEach(message -> {
                    System.out.println("FromAbstractionId: " + message.getFromAbstractionId() + "ToAbstractionId: " + message.getToAbstractionId());
                    if (!abstractions.containsKey(message.getToAbstractionId())) {
                        //TODO register additional abstraction handlers - for nnar & uc
                    }
                    if (abstractions.get(message.getToAbstractionId()).handle(message)) {
                        System.out.println("Handled " + message.getType());
                        messageQueue.remove(message);
                    }
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

    public void registerAbstraction(Abstraction abstraction) {
        abstractions.put(abstraction.getAbstractionId(), abstraction);
    }

    public void addMessageToQueue(Protocol.Message message) {
        messageQueue.add(message);
    }

    private void register() {
        Protocol.ProcRegistration procRegistration = Protocol.ProcRegistration
                .newBuilder()
                .setOwner(process.getOwner())
                .setIndex(process.getIndex())
                .build();

        Protocol.Message procRegistrationMessage = Protocol.Message
                .newBuilder()
                .setType(Protocol.Message.Type.PROC_REGISTRATION)
                .setProcRegistration(procRegistration)
                .setMessageUuid(UUID.randomUUID().toString())
                .setToAbstractionId(AbstractionType.APP.getId())
                .setSystemId(ProcessConstants.SYSTEM_ID)
                .build();

        Protocol.NetworkMessage networkMessage = Protocol.NetworkMessage
                .newBuilder()
                .setSenderHost(process.getHost())
                .setSenderListeningPort(process.getPort())
                .setMessage(procRegistrationMessage)
                .build();

        Protocol.Message outerMessage = Protocol.Message
                .newBuilder()
                .setType(Protocol.Message.Type.NETWORK_MESSAGE)
                .setNetworkMessage(networkMessage)
                .setToAbstractionId(procRegistrationMessage.getToAbstractionId())
//                .setSystemId(ProcessConstants.SYSTEM_ID)
                .setMessageUuid(UUID.randomUUID().toString())
                .build();

        MessageSender.send(outerMessage, HubInfo.HOST, HubInfo.PORT);
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
        if (arg instanceof Protocol.Message) {
            Protocol.Message message = (Protocol.Message) arg;
            Protocol.Message innerMessage = message.getNetworkMessage().getMessage();
            if (Protocol.Message.Type.PROC_INITIALIZE_SYSTEM.equals(innerMessage.getType())) {
                handleProcInitializeSystem(innerMessage);
            } else {
                messageQueue.add(message);
            }
        }
    }

    private void handleProcInitializeSystem(Protocol.Message message) {
        Protocol.ProcInitializeSystem procInitializeSystem = message.getProcInitializeSystem();
        this.processes = procInitializeSystem.getProcessesList();
    }
}
