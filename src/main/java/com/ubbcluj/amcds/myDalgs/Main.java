package com.ubbcluj.amcds.myDalgs;

import com.ubbcluj.amcds.myDalgs.communication.Protocol;
import com.ubbcluj.amcds.myDalgs.globals.HubInfo;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

public class Main {

    public static void main(String[] args) throws InterruptedException {

        String hubHost = args[0];
        int hubPort = Integer.parseInt(args[1]);
        HubInfo.initHub(hubHost, hubPort);

        final String processHost = args[2];
        final List<Integer> processPorts = Arrays.asList(Integer.parseInt(args[3]), Integer.parseInt(args[4]), Integer.parseInt(args[5]));
        final String processOwner = args[6];

        List<Protocol.ProcessId> processIds = processPorts.stream()
                .map(port -> Protocol.ProcessId.newBuilder()
                        .setHost(processHost)
                        .setPort(port)
                        .setOwner(processOwner)
                        .setIndex(processPorts.indexOf(port) + 1)
                        .build())
                .collect(Collectors.toList());

        Thread process1 = new Thread(new Process(processIds.get(0)));
        Thread process2 = new Thread(new Process(processIds.get(1)));
        Thread process3 = new Thread(new Process(processIds.get(2)));

        process1.start();
        process2.start();
        process3.start();

        process1.join();
        process2.join();
        process3.join();
    }
}
