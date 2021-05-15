package com.ubbcluj.amcds.myDalgs.util;

import com.ubbcluj.amcds.myDalgs.communication.Protocol;

import java.util.Collection;
import java.util.Comparator;
import java.util.Set;

public class DalgsUtil {

    public static Protocol.Value buildUndefinedValue() {
        return Protocol.Value
                .newBuilder()
                .setDefined(false)
                .build();
    }

    public static Protocol.ProcessId getMaxRankedProcess(Collection<Protocol.ProcessId> processes) {
        return processes.stream()
                .max(Comparator.comparing(Protocol.ProcessId::getRank))
                .orElse(null);
    }
}
