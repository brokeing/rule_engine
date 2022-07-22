package org.why.core;

public class Engine {
    public static void run() throws Exception {
        // TODO here load config
        ProcessStream processStream = new ProcessStream();
        processStream.build().start();
    }
}
