package com.ubbcluj.amcds.myDalgs.algorithms;

import com.ubbcluj.amcds.myDalgs.communication.Protocol;

public class PerfectLink extends Abstraction {

    @Override
    public boolean canHandle(Protocol.Message message) {
        return true;
    }

    @Override
    public void handle(Protocol.Message message) {

    }
}
