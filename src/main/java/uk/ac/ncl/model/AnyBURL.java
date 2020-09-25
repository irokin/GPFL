package uk.ac.ncl.model;

import com.google.common.collect.BiMap;
import com.google.common.collect.HashBiMap;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import uk.ac.ncl.Settings;
import uk.ac.ncl.core.Context;
import uk.ac.ncl.core.Engine;
import uk.ac.ncl.core.GlobalTimer;
import uk.ac.ncl.core.GraphOps;
import uk.ac.ncl.structure.Pair;
import uk.ac.ncl.structure.Rule;
import uk.ac.ncl.utils.Helpers;
import uk.ac.ncl.utils.IO;
import uk.ac.ncl.utils.Logger;

import java.io.File;
import java.text.MessageFormat;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingDeque;

public class AnyBURL extends Engine {
    public AnyBURL(File config) {
        super(config, "anyburl");
        Helpers.reportSettings();
    }

    BiMap<String, Long> nodeIndex = HashBiMap.create();

    private void indexing() {
        try(Transaction tx = graph.beginTx()) {
            for (Node node : graph.getAllNodes()) {
                nodeIndex.put(GraphOps.readNeo4jProperty(node), node.getId());
            }
            tx.success();
        }
        Logger.println("# Indexed " + nodeIndex.size() + " nodes.");
    }

    public void learn() {
        graph = IO.loadGraph(new File(home, "databases/graph.db"));
        trainFile = new File(home, "data/train.txt");
        validFile = new File(home, "data/valid.txt");
        testFile = new File(home, "data/test.txt");
        ruleFile = IO.createEmptyFile(new File(out, "rules.txt"));
        indexing();

        populateTargets();
        GlobalTimer.programStartTime = System.currentTimeMillis();

        for (String target : targets) {
            Settings.TARGET = target;
            Context context = new Context();
            Logger.println(MessageFormat.format("\n# ({0}\\{1}) Start Learning Rules for Target: {2}",
                    globalTargetCounter++, targets.size(), target), 1);

            Set<Pair> trainPairs = IO.readExamples(trainFile, nodeIndex);
            Set<Pair> validPairs = IO.readExamples(validFile, nodeIndex);
            Set<Pair> testPairs = IO.readExamples(testFile, nodeIndex);

            Logger.println(MessageFormat.format("# Train Size: {0} | Valid Size: {1} | Test Size: {2}"
                    , trainPairs.size(), validPairs.size(), testPairs.size()), 1);

            GraphOps.removeRelationships(validPairs, graph);
            GraphOps.removeRelationships(testPairs, graph);

            BlockingQueue<Rule> ruleQueue = new LinkedBlockingDeque<>();
            RuleProducer[] producers = new RuleProducer[Settings.THREAD_NUMBER];
            for (int i = 0; i < producers.length; i++) {
            }

            IO.writeRules(ruleFile, context.topRules);

            GraphOps.addRelationships(validPairs, graph);
            GraphOps.addRelationships(testPairs, graph);
        }
    }

    class RuleProducer extends Thread {
        RuleConsumer consumer;

        public RuleProducer(int id, RuleConsumer consumer) {
            super("RuleProducer-" + id);
            start();
        }

        @Override
        public void run() {
            super.run();
        }
    }

    class RuleConsumer extends Thread {
        public RuleConsumer() {
            super("RuleConsumer");
            start();
        }

        @Override
        public void run() {
            super.run();
        }
    }


}
