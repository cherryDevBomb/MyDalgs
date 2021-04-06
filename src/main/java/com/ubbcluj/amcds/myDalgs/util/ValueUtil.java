package com.ubbcluj.amcds.myDalgs.util;

import com.ubbcluj.amcds.myDalgs.communication.Protocol;

public class ValueUtil {

    public static Protocol.Value buildUndefinedValue() {
        return Protocol.Value
                .newBuilder()
                .setDefined(false)
                .build();
    }
}
