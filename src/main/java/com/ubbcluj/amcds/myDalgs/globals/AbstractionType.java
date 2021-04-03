package com.ubbcluj.amcds.myDalgs.globals;

public enum AbstractionType {
    PL("pl"),
    BEB("beb"),
    APP("app");

    private String id;

    private AbstractionType(String id) {
        this.id = id;
    }

    public String getId() {
        return id;
    }
}
