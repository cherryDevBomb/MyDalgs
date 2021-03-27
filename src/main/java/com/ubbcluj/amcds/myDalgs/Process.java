package com.ubbcluj.amcds.myDalgs;

import com.ubbcluj.amcds.myDalgs.communication.CommunicationProtocol;

public class Process implements Runnable {

    private CommunicationProtocol.ProcessId process;

    private final String systemId = "sys-1";

    public Process(CommunicationProtocol.ProcessId process) {
        this.process = process;

    }

    public void run() {
        System.out.println("Running process " + process.getOwner() + "-" + process.getIndex());
    }

}
