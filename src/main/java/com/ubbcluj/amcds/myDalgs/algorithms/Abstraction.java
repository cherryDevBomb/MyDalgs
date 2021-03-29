package com.ubbcluj.amcds.myDalgs.algorithms;

import com.ubbcluj.amcds.myDalgs.communication.Protocol;

import java.util.Observable;

public abstract class Abstraction extends Observable {

    public abstract boolean canHandle(Protocol.Message message);

    public abstract void handle(Protocol.Message message);

    /**
     * Override to always call setChanged() before notifying the observer process of an incoming message.
     * Without setChanged() notifyObservers() will be ignored.
     *
     * @param arg
     */
    @Override
    public void notifyObservers(Object arg) {
        setChanged();
        super.notifyObservers(arg);
    }
}
