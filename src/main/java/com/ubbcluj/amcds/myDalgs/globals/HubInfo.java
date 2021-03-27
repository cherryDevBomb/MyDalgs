package com.ubbcluj.amcds.myDalgs.globals;

public class HubInfo {

    public static String HOST;
    public static int PORT;

    public static void initHub(String host, int port) {
        HOST = host;
        PORT = port;
    }
}
