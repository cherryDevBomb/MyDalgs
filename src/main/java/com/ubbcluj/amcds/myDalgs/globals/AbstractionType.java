package com.ubbcluj.amcds.myDalgs.globals;

public enum AbstractionType {
    PL("pl"),
    BEB("beb"),
    APP("app"),
    NNAR("nnar");

    private final String id;

    private AbstractionType(String id) {
        this.id = id;
    }

    public String getId() {
        return id;
    }
}
