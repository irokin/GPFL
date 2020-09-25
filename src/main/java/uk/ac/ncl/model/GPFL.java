package uk.ac.ncl.model;

import uk.ac.ncl.Settings;
import uk.ac.ncl.core.*;
import uk.ac.ncl.utils.Helpers;
import uk.ac.ncl.utils.IO;
import uk.ac.ncl.utils.Logger;
import com.google.common.collect.Multimap;
import org.neo4j.graphdb.Transaction;
import uk.ac.ncl.structure.Pair;
import uk.ac.ncl.structure.Triple;

import java.io.File;
import java.text.MessageFormat;
import java.util.Set;

public class GPFL extends Engine {

    public GPFL(File config, String logName) {
        super(config, logName);
        Helpers.reportSettings();
    }

    public void run() {
        graphFile = new File(home, "databases/graph.db");
        graph = IO.loadGraph(graphFile);
        trainFile = new File(home, "data/annotated_train.txt");
        validFile = new File(home, "data/annotated_valid.txt");
        testFile = new File(home, "data/annotated_test.txt");
        ruleFile = IO.createEmptyFile(new File(out, "rules.txt"));
        predictionFile = IO.createEmptyFile(new File(out, "predictions.txt"));
        verificationFile = IO.createEmptyFile(new File(out, "verifications.txt"));
        ruleIndexHome = new File(out, "index");
        ruleIndexHome.mkdir();

        populateTargets();
        GlobalTimer.programStartTime = System.currentTimeMillis();

        for (String target : targets) {
            File ruleIndexFile = IO.createEmptyFile(new File(ruleIndexHome
                    , target.replaceAll("[:/]", "_") + ".txt"));
            Settings.TARGET = target;
            Context context = new Context();
            Logger.println(MessageFormat.format("\n# ({0}\\{1}) Start Learning Rules for Target: {2}",
                    globalTargetCounter++, targets.size(), target), 1);

            try (Transaction tx = graph.beginTx()) {
                Set<Pair> trainPairs = IO.readPair(graph, trainFile, target);
                Settings.TARGET_FUNCTIONAL = IO.isTargetFunctional(trainPairs);
                Set<Pair> validPairs = IO.readPair(graph, validFile, target);
                Set<Pair> testPairs = IO.readPair(graph, testFile, target);

                Logger.println(MessageFormat.format("# Functional: {0} | Train Size: {1} | Valid Size: {2} | Test Size: {3}"
                        , Settings.TARGET_FUNCTIONAL, trainPairs.size(), validPairs.size(), testPairs.size()), 1);

                Set<Pair> filterSet = Helpers.combine(trainPairs, validPairs, testPairs);
                generalization(trainPairs, context);
                if(Settings.ESSENTIAL_TIME != -1 && Settings.INS_DEPTH != 0)
                    EssentialRuleGenerator.generateEssentialRules(trainPairs, validPairs
                            , context, graph, ruleIndexFile, ruleFile);
                specialization(context, trainPairs, validPairs, ruleIndexFile);
                IO.orderRuleIndexFile(ruleIndexFile);

                ruleApplication(context, ruleIndexFile);
                Evaluator evaluator = new Evaluator(testPairs, filterSet, context, predictionFile, verificationFile, graph);
                evaluator.createQueries();
                tx.success();
            }
        }
        GlobalTimer.reportMaxMemoryUsed();
        GlobalTimer.reportTime();

        Logger.init(new File(out, "eval_log.txt"), false);
        Multimap<String, Triple> filterMap = Evaluator.buildFilterMap(home.getPath());
        Evaluator.scoreGPFL(filterMap, predictionFile);
    }

    public void learn() {
        graphFile = new File(home, "databases/graph.db");
        graph = IO.loadGraph(graphFile);

        trainFile = new File(home, "data/annotated_train.txt");
        validFile = new File(home, "data/annotated_valid.txt");
        ruleFile = IO.createEmptyFile(new File(out, "rules.txt"));
        ruleIndexHome = new File(out, "index");
        ruleIndexHome.mkdir();
        populateTargets();
        GlobalTimer.programStartTime = System.currentTimeMillis();

        for (String target : targets) {
            File ruleIndexFile = IO.createEmptyFile(new File(ruleIndexHome
                    , target.replaceAll("[:/<>]", "_") + ".txt"));

            Settings.TARGET = target;
            Context context = new Context();
            Logger.println(MessageFormat.format("\n# ({0}\\{1}) Start Learning Rules for Target: {2}",
                    globalTargetCounter++, targets.size(), target), 1);

            try (Transaction tx = graph.beginTx()) {
                Set<Pair> trainPairs = IO.readPair(graph, trainFile, target);
                Settings.TARGET_FUNCTIONAL = IO.isTargetFunctional(trainPairs);
                Set<Pair> validPairs = IO.readPair(graph, validFile, target);

                Logger.println(MessageFormat.format("# Train Size: {0}", trainPairs.size()), 1);

                generalization(trainPairs, context);
                if(Settings.ESSENTIAL_TIME != -1 && Settings.INS_DEPTH != 0)
                    EssentialRuleGenerator.generateEssentialRules(trainPairs, validPairs, context, graph, ruleIndexFile, ruleFile);
                specialization(context, trainPairs, validPairs, ruleIndexFile);

                IO.orderRuleIndexFile(ruleIndexFile);
                tx.success();
            }
        }

        IO.orderRules(out);
        GlobalTimer.reportMaxMemoryUsed();
        GlobalTimer.reportTime();
    }

    public void apply() {
        graphFile = new File(home, "databases/graph.db");
        graph = IO.loadGraph(graphFile);
        trainFile = new File(home, "data/annotated_train.txt");
        validFile = new File(home, "data/annotated_valid.txt");
        testFile = new File(home, "data/annotated_test.txt");

        predictionFile = IO.createEmptyFile(new File(out, "predictions.txt"));
        verificationFile = IO.createEmptyFile(new File(out, "verifications.txt"));
        ruleIndexHome = new File(out, "index");
        populateTargets();
        GlobalTimer.programStartTime = System.currentTimeMillis();

        for (String target : targets) {
            File ruleIndexFile = new File(ruleIndexHome
                    , target.replaceAll("[:/]", "_") + ".txt");
            if(!ruleIndexFile.exists())
                continue;

            Settings.TARGET = target;
            Context context = new Context();
            Logger.println(MessageFormat.format("\n# ({0}\\{1}) Start Applying Rules for Target: {2}",
                    globalTargetCounter++, targets.size(), target), 1);

            try (Transaction tx = graph.beginTx()) {
                Set<Pair> trainPairs = IO.readPair(graph, trainFile, target);
                Set<Pair> validPairs = IO.readPair(graph, validFile, target);
                Set<Pair> testPairs = IO.readPair(graph, testFile, target);
                Set<Pair> filterSet = Helpers.combine(trainPairs, validPairs, testPairs);
                Logger.println(MessageFormat.format("# Train Size: {0} | " + "Valid Size: {1} | " + "Test Size: {2}"
                        , trainPairs.size(), validPairs.size(), testPairs.size()), 1);

                ruleApplication(context, ruleIndexFile);
                Evaluator evaluator = new Evaluator(testPairs, filterSet, context, predictionFile, verificationFile, graph);
                evaluator.createQueries();
                tx.success();
            }
        }

        GlobalTimer.reportMaxMemoryUsed();
        GlobalTimer.reportTime();

        Logger.init(new File(out, "eval_log.txt"), false);
        Multimap<String, Triple> filterMap = Evaluator.buildFilterMap(home.getPath());
        Evaluator.scoreGPFL(filterMap, predictionFile);
    }
}
