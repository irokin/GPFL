package uk.ac.ncl.core;

import uk.ac.ncl.Settings;
import uk.ac.ncl.structure.Stamp;
import uk.ac.ncl.utils.Logger;
import uk.ac.ncl.utils.MathUtils;

import java.text.DecimalFormat;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

public class GlobalTimer {
    static DecimalFormat f = new DecimalFormat("####.###");

    private static long specStartTime = 0;
    private static long genStartTime = 0;
    private static long essentialStartTime = 0;

    public static long programStartTime = 0;

    public static List<Double> generalizationTime = new ArrayList<>();
    public static List<Double> generalizationMem = new ArrayList<>();
    public static List<Double> specializationTime = new ArrayList<>();
    public static List<Double> specializationMem = new ArrayList<>();
    public static List<Double> genEssentialTime = new ArrayList<>();
    public static List<Double> genEssentialMem = new ArrayList<>();
    public static List<Double> ruleApplyTime = new ArrayList<>();
    public static List<Double> ruleApplyMem = new ArrayList<>();
    public static List<Double> allMem = new ArrayList<>();
    public static List<Double> allTime = new ArrayList<>();


    public static void updateGenEssentialStats(Stamp stamp) {
        genEssentialTime.add(stamp.time);
        genEssentialMem.add(stamp.mem);
        allTime.add(stamp.time);
        allMem.add(stamp.mem);
    }

    public static void updateTemplateGenStats(Stamp stamp) {
        generalizationTime.add(stamp.time);
        generalizationMem.add(stamp.mem);
        allTime.add(stamp.time);
        allMem.add(stamp.mem);
    }

    public static void updateInsRuleStats(Stamp stamp) {
        specializationTime.add(stamp.time);
        specializationMem.add(stamp.mem);
        allTime.add(stamp.time);
        allMem.add(stamp.mem);
    }

    public static void updateRuleApplyStats(Stamp stamp) {
        ruleApplyTime.add(stamp.time);
        ruleApplyMem.add(stamp.mem);
        allTime.add(stamp.time);
        allMem.add(stamp.mem);
    }

    public static void reportMaxMemoryUsed() {
        allMem.sort(Comparator.reverseOrder());
        double value = allMem.isEmpty() ? 0 : allMem.get(0);
        Logger.println("\n# Memory Usage: " + f.format(value) + "mb");
    }

    public static void reportTime() {
        double totalRuntime = (double) (System.currentTimeMillis() - programStartTime) / 1000;
        Logger.println(MessageFormat.format("# Runtime: Total = {0}s | Avg per Target = {1}s\n" +
                "# Generalization: Total = {2}s | Avg per Target = {3}s\n" +
                "# Essential: Total = {8}s | Avg per Target = {9}s\n" +
                "# specialization: Total = {4}s | Avg per Target = {5}s\n" +
                "# Rule Application: Total = {6}s | Avg per Target = {7}s\n"
                , f.format(totalRuntime), f.format(totalRuntime / generalizationTime.size())
                , f.format(MathUtils.listSum(generalizationTime)), f.format(MathUtils.listMean(generalizationTime))
                , f.format(MathUtils.listSum(specializationTime)), f.format(MathUtils.listMean(specializationTime))
                , f.format(MathUtils.listSum(ruleApplyTime)), f.format(MathUtils.listMean(ruleApplyTime))
                , f.format(MathUtils.listSum(genEssentialTime)), f.format(MathUtils.listMean(genEssentialTime))
                ), 2);
    }

    public static boolean stopSpec() {
        if(Settings.SPEC_TIME == 0)
            return false;
        return ((double) (System.currentTimeMillis() - specStartTime) / 1000d) > Settings.SPEC_TIME;
    }

    public static void setSpecStartTime(long insStartTime) {
        GlobalTimer.specStartTime = insStartTime;
    }

    public static void setEssentialStartTime(long startTime) {
        GlobalTimer.essentialStartTime = startTime;
    }

    public static boolean stopEssential() {
        if(Settings.ESSENTIAL_TIME == 0)
            return false;
        return ((double) (System.currentTimeMillis() - essentialStartTime) / 1000d) > Settings.ESSENTIAL_TIME;
    }

    public static void setGenStartTime(long genStartTime) {
        GlobalTimer.genStartTime = genStartTime;
    }

    public static boolean stopGen() {
        if(Settings.GEN_TIME == 0)
            return false;
        return ((double) (System.currentTimeMillis() - genStartTime) / 1000d) > Settings.GEN_TIME;
    }

}
