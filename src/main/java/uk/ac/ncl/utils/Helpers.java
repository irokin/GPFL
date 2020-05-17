package uk.ac.ncl.utils;

import uk.ac.ncl.Settings;
import uk.ac.ncl.structure.Stamp;
import org.json.JSONObject;
import org.json.JSONTokener;

import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.lang.management.ManagementFactory;
import java.text.DecimalFormat;
import java.text.MessageFormat;
import java.util.HashSet;
import java.util.Set;

public class Helpers {
    public static Stamp timerAndMemory(long s, String m) {
        DecimalFormat f = new DecimalFormat("####.###");
        Runtime r = Runtime.getRuntime();
        double time = (double) (System.currentTimeMillis() - s) / 1000;
        double memory = ((double) r.totalMemory() - r.freeMemory()) / (1024L * 1024L);
        Logger.println(MessageFormat.format("{0}: time = {1}s | memory = {2}mb"
                , m
                , f.format(time)
                , f.format(memory)), 2);
        return new Stamp(time, memory);
    }

    public static JSONObject buildJSONObject(File file) {
        JSONObject args = null;

        try (InputStream in = new FileInputStream(file)) {
            JSONTokener tokener = new JSONTokener(in);
            args = new JSONObject(tokener);
        } catch (Exception e) {
            e.printStackTrace();
        }

        return args;
    }

    public static void cleanDirectories(File root) {
        for (File cv : root.listFiles()) {
            for (File data : cv.listFiles()) {
                data.delete();
            }
            cv.delete();
        }
    }

    @SafeVarargs
    public static <T> Set<T> combine(Set<T>... sources) {
        Set<T> dest = new HashSet<>();
        for (Set<T> source : sources) {
            dest.addAll(source);
        }
        return dest;
    }

    public static <T> Set<T> joinSets(Set<T> s1, Set<T> s2) {
        Set<T> joined = new HashSet<>();
        for (T t : s1) {
            if(s2.contains(t))
                joined.add(t);
        }
        return joined;
    }

    public static double formatDouble(DecimalFormat format, double value) {
        return Double.parseDouble(format.format(value));
    }

    public static boolean readSetting(JSONObject args, String key, boolean defaultValue) {
        if(args.has(key)) return args.getBoolean(key);
        else return defaultValue;
    }

    public static int readSetting(JSONObject args, String key, int defaultValue) {
        if(args.has(key)) return args.getInt(key);
        else return defaultValue;
    }

    public static double readSetting(JSONObject args, String key, double defaultValue) {
        if(args.has(key)) return args.getDouble(key);
        else return defaultValue;
    }

    public static String readSetting(JSONObject args, String key, String defaultValue) {
        if(args.has(key)) return args.getString(key);
        else return defaultValue;
    }

    public static int readSettingConditionMax(JSONObject args, String key, int defaultValue) {
        if(args.has(key))
            return args.getInt(key) == 0 ? Integer.MAX_VALUE : args.getInt(key);
        return defaultValue;
    }

    public static long JVMRam() {
        long ram = Runtime.getRuntime().maxMemory();
        return (long) Math.ceil(ram / (1024d * 1024 * 1024));
    }

    public static long systemRAM() {
        long memorySize = ((com.sun.management.OperatingSystemMXBean) ManagementFactory
                .getOperatingSystemMXBean()).getTotalPhysicalMemorySize();
        return (long) Math.ceil(memorySize / (1024d * 1024 * 1024));
    }

    public static void reportSettings() {
        String msg = MessageFormat.format("\n# Settings:\n" +
                "# Ins Length = {0} | CAR Length = {1}\n" +
                "# Support = {2} | Confidence = {3}\n" +
                "# Head Coverage = {4} | Learn Groundings = {5}\n" +
                "# Apply Groundings = {6} | Saturation = {7}\n" +
                "# Batch Size = {8} | Spec Time = {9}\n" +
                "# Neo4J Identifier = {10} | Confidence Offset = {11}\n" +
                "# Suggestion Cap = {12} | Threads = {13}\n" +
                "# Random Walkers = {14} | Essential Time = {15}\n" +
                "# Ins Rule Cap = {16} | Gen Time = {17}\n" +
                "# Quality Measure = {18} | Overfitting Factor = {19}"
                , Settings.INS_DEPTH
                , Settings.CAR_DEPTH
                , Settings.SUPPORT
                , String.valueOf(Settings.CONF)
                , Settings.HEAD_COVERAGE
                , Settings.LEARN_GROUNDINGS == Integer.MAX_VALUE ? "Max" : Settings.LEARN_GROUNDINGS
                , Settings.APPLY_GROUNDINGS == Integer.MAX_VALUE ? "Max" : Settings.APPLY_GROUNDINGS
                , Settings.SATURATION
                , Settings.BATCH_SIZE
                , Settings.SPEC_TIME == Integer.MAX_VALUE ? "Max" : Settings.SPEC_TIME
                , Settings.NEO4J_IDENTIFIER
                , Settings.CONFIDENCE_OFFSET
                , Settings.SUGGESTION_CAP == Integer.MAX_VALUE ? "Max" : Settings.SUGGESTION_CAP
                , Settings.THREAD_NUMBER
                , Settings.RANDOM_WALKERS
                , Settings.ESSENTIAL_TIME == Integer.MAX_VALUE ? "Max" : Settings.ESSENTIAL_TIME
                , Settings.INS_RULE_CAP == Integer.MAX_VALUE ? "Max" : Settings.INS_RULE_CAP
                , Settings.GEN_TIME == Integer.MAX_VALUE ? "Max" : Settings.GEN_TIME
                , Settings.QUALITY_MEASURE
                , Settings.OVERFITTING_FACTOR
        );
        Logger.println(msg, 1);
    }
}


