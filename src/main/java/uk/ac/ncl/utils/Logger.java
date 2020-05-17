package uk.ac.ncl.utils;

import uk.ac.ncl.Settings;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;

public class Logger {
    static File logFile;

    public static void init(File f, boolean append) {
        logFile = f;
        if(!append) overwrite();
    }

    public static void println(String msg) {
        println(msg, 1);
    }

    public static void println(String msg, int verbosity) {
        if(verbosity <= Settings.VERBOSITY) {
            System.out.println(msg);
        }

        if(logFile != null) {
            try (PrintWriter writer = new PrintWriter(new FileWriter(logFile, true), true)) {
                writer.println(msg);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    public static void overwrite() {
        try( PrintWriter pw = new PrintWriter(logFile) ) {
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
