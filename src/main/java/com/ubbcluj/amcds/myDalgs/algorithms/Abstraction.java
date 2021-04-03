package com.ubbcluj.amcds.myDalgs.algorithms;

import com.ubbcluj.amcds.myDalgs.communication.Protocol;

public abstract class Abstraction {

    protected String abstractionId;
    protected com.ubbcluj.amcds.myDalgs.Process process;

    protected Abstraction(String abstractionId, com.ubbcluj.amcds.myDalgs.Process process) {
        this.abstractionId = abstractionId;
        this.process = process;
    }

    public String getAbstractionId() {
        return abstractionId;
    }

    public abstract boolean handle(Protocol.Message message);
}
