package com.ubbcluj.amcds.myDalgs.model;

import com.ubbcluj.amcds.myDalgs.communication.Protocol;

public class EpState {

    private int valTimestamp;
    private Protocol.Value val;

    public EpState(int valTimestamp, Protocol.Value val) {
        this.valTimestamp = valTimestamp;
        this.val = val;
    }

    public int getValTimestamp() {
        return valTimestamp;
    }

    public void setValTimestamp(int valTimestamp) {
        this.valTimestamp = valTimestamp;
    }

    public Protocol.Value getVal() {
        return val;
    }

    public void setVal(Protocol.Value val) {
        this.val = val;
    }
}
