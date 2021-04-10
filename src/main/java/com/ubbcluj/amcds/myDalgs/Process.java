package com.ubbcluj.amcds.myDalgs;

import com.ubbcluj.amcds.myDalgs.algorithms.Abstraction;
import com.ubbcluj.amcds.myDalgs.algorithms.Application;
import com.ubbcluj.amcds.myDalgs.algorithms.NNAtomicRegister;
import com.ubbcluj.amcds.myDalgs.communication.Protocol;
import com.ubbcluj.amcds.myDalgs.model.AbstractionType;
import com.ubbcluj.amcds.myDalgs.network.MessageReceiver;
import com.ubbcluj.amcds.myDalgs.network.MessageSender;
import com.ubbcluj.amcds.myDalgs.util.AbstractionIdUtil;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;

public class Process implements Runnable, Observer {

    private Protocol.ProcessId process;

    private Protocol.ProcessId hub;
    private List<Protocol.ProcessId> processes;
    private String systemId;
    private final Queue<Protocol.Message> messageQueue;
    private final Map<String, Abstraction> abstractions;

    public Process(Protocol.ProcessId process, Protocol.ProcessId hub) {
        this.process = process;
        this.hub = hub;
        this.messageQueue = new ConcurrentLinkedQueue<>();
        this.abstractions = new ConcurrentHashMap<>();
    }

    @Override
    public void run() {
        System.out.println("Running process " + process.getOwner() + "-" + process.getIndex());

        //register abstractions
        registerAbstraction(new Application(AbstractionType.APP.getId(), this));

        // start event loop
        Runnable eventLoop = () -> {
            while (true) {
                messageQueue.forEach(message -> {
                    System.out.println(process.getOwner() + "-" + process.getIndex() + " handling " + message.getType() + "; FromAbstractionId: " + message.getFromAbstractionId() + "; ToAbstractionId: " + message.getToAbstractionId());
                    if (!abstractions.containsKey(message.getToAbstractionId())) {
//                        //TODO register additional abstraction handlers - for nnar & uc
                        if (message.getToAbstractionId().contains(AbstractionType.NNAR.getId())) {
                            registerAbstraction(new NNAtomicRegister(AbstractionIdUtil.getNamedAncestorAbstractionId(message.getToAbstractionId()), this));
                        }
                    }
                    if (abstractions.get(message.getToAbstractionId()).handle(message)) {
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
        abstractions.putIfAbsent(abstraction.getAbstractionId(), abstraction);
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
                .setMessageUuid(UUID.randomUUID().toString())
                .build();

        MessageSender.send(outerMessage, hub.getHost(), hub.getPort());
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
        this.process = getProcessByHostAndPort(this.process.getHost(), this.process.getPort()).get();
        this.systemId = message.getSystemId();
    }

    public Optional<Protocol.ProcessId> getProcessByHostAndPort(String host, int port) {
        return processes.stream()
                .filter(p -> host.equals(p.getHost()) && p.getPort() == port)
                .findFirst();
    }

    public Protocol.ProcessId getProcess() {
        return process;
    }

    public Protocol.ProcessId getHub() {
        return hub;
    }

    public List<Protocol.ProcessId> getProcesses() {
        return processes;
    }

    public String getSystemId() {
        return systemId;
    }
}
