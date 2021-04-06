package com.ubbcluj.amcds.myDalgs.model;

import com.ubbcluj.amcds.myDalgs.communication.Protocol;
import com.ubbcluj.amcds.myDalgs.util.ValueUtil;

public class NNARValue {

    private int timestamp;
    private int writerRank;
    private Protocol.Value value;

    public NNARValue() {
        timestamp = 0;
        writerRank = 0;
        value = ValueUtil.buildUndefinedValue();
    }

    public NNARValue(int timestamp, int writerRank, Protocol.Value value) {
        this.timestamp = timestamp;
        this.writerRank = writerRank;
        this.value = value;
    }

    public int getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(int timestamp) {
        this.timestamp = timestamp;
    }

    public int getWriterRank() {
        return writerRank;
    }

    public void setWriterRank(int writerRank) {
        this.writerRank = writerRank;
    }

    public Protocol.Value getValue() {
        return value;
    }

    public void setValue(Protocol.Value value) {
        this.value = value;
    }
}
