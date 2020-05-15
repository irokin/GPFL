package ac.uk.ncl.core;

import ac.uk.ncl.Settings;
import ac.uk.ncl.structure.*;
import ac.uk.ncl.utils.Helpers;
import ac.uk.ncl.utils.Logger;
import ac.uk.ncl.utils.SemaphoredThreadPool;
import com.google.common.collect.Multimap;
import com.google.common.collect.MultimapBuilder;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.Transaction;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.text.DecimalFormat;
import java.text.NumberFormat;
import java.util.Collection;
import java.util.HashSet;
import java.util.Locale;
import java.util.Set;
import java.util.concurrent.*;

public class EssentialRuleGenerator {

    public static void generateEssentialRules(Set<Pair> trainPairs, Set<Pair> validPairs
            , Context context, GraphDatabaseService graph
            , File tempFile, File ruleFile) {
        long s = System.currentTimeMillis();
        NumberFormat f = NumberFormat.getNumberInstance(Locale.US);
        GlobalTimer.setEssentialStartTime(System.currentTimeMillis());

        Set<Rule> essentialRules = new HashSet<>();
        BlockingQueue<String> tempFileContents = new LinkedBlockingDeque<>(10000000);
        BlockingQueue<String> ruleFileContents = new LinkedBlockingDeque<>(10000000);

        for (Rule rule : context.getAbstractRules()) {
            if(!rule.isClosed() && rule.length() == 1)
                essentialRules.add(rule);
        }

        Multimap<Long, Long> trainObjToSub = MultimapBuilder.hashKeys().hashSetValues().build();
        Multimap<Long, Long> validObjToSub = MultimapBuilder.hashKeys().hashSetValues().build();
        Multimap<Long, Long> trainSubToObj = MultimapBuilder.hashKeys().hashSetValues().build();
        Multimap<Long, Long> validSubToObj = MultimapBuilder.hashKeys().hashSetValues().build();

        for (Pair trainPair : trainPairs) {
            trainObjToSub.put(trainPair.objId, trainPair.subId);
            trainSubToObj.put(trainPair.subId, trainPair.objId);
        }

        for (Pair validPair : validPairs) {
            validObjToSub.put(validPair.objId, validPair.subId);
            validSubToObj.put(validPair.subId, validPair.objId);
        }

        ExecutorService executors = new SemaphoredThreadPool(Settings.THREAD_NUMBER);
        RuleWriter tempFileWriter = new RuleWriter(0, executors, tempFile, tempFileContents, true);
        RuleWriter ruleFileWriter = new RuleWriter(0, executors, ruleFile, ruleFileContents, true);
        Set<Rule> specializedRules = new HashSet<>();

        try {
            for (Rule rule : essentialRules) {
                if(GlobalTimer.stopEssential()) break;
                BlockingQueue<String> contents = new LinkedBlockingDeque<>();
                Set<Future<?>> futures = new HashSet<>();
                context.ruleFrequency.remove(rule);

                Set<Pair> groundings = generateBodyGrounding(rule, graph);
                Multimap<Long, Long> originalToTails = MultimapBuilder.hashKeys().hashSetValues().build();
                Multimap<Long, Long> tailToOriginals = MultimapBuilder.hashKeys().hashSetValues().build();

                for (Pair grounding : groundings) {
                    originalToTails.put(grounding.subId, grounding.objId);
                    tailToOriginals.put(grounding.objId, grounding.subId);
                }

                Multimap<Long, Long> trainAnchoringToOriginals = rule.isFromSubject() ? trainObjToSub : trainSubToObj;
                Multimap<Long, Long> validAnchoringToOriginals = rule.isFromSubject() ? validObjToSub : validSubToObj;
                for (Long anchoring : trainAnchoringToOriginals.keySet()) {
                    if(GlobalTimer.stopEssential()) break;
                    Collection<Long> validOriginals = validAnchoringToOriginals.get(anchoring);
                    futures.add(executors.submit(new CreateHAR(rule, anchoring, trainAnchoringToOriginals.get(anchoring)
                             , validOriginals, originalToTails.keySet()
                             , graph, contents, ruleFileContents, context)));

                    Set<Pair> candidates = new HashSet<>();
                    for (Long original : trainAnchoringToOriginals.get(anchoring)) {
                        if(GlobalTimer.stopEssential()) break;
                        for (Long tail : originalToTails.get(original)) {
                            if(GlobalTimer.stopEssential()) break;
                            Pair candidate = new Pair(anchoring, tail);
                            if (!candidates.contains(candidate) && !trivialCheck(rule, anchoring, tail)) {
                                candidates.add(candidate);
                                futures.add(executors.submit(new CreateBAR(rule, candidate, trainAnchoringToOriginals.get(anchoring)
                                        , validOriginals, tailToOriginals.get(tail)
                                        , graph, contents, ruleFileContents, context)));
                            }
                        }
                    }
                }

                for (Future<?> future : futures) {
                    future.get();
                }

                if(!contents.isEmpty()) {
                    specializedRules.add(rule);
                    rule.stats.compute();
                    tempFileContents.put("ABS: " + context.getIndex(rule) + "\t"
                            + ((Template) rule).toRuleIndexString() + "\t"
                            + f.format(rule.getStandardConf()) + "\t"
                            + f.format(rule.getSmoothedConf()) + "\t"
                            + f.format(rule.getPcaConf()) + "\t"
                            + f.format(rule.getApcaConf()) + "\t"
                            + f.format(rule.getHeadCoverage()) + "\t"
                            + f.format(rule.getValidPrecision()) + "\n"
                            + String.join("\t", contents) + "\n");
                }
            }

            executors.shutdown();
            executors.awaitTermination(1L, TimeUnit.MINUTES);
            tempFileWriter.join();
            ruleFileWriter.join();
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(-1);
        }

        GlobalTimer.updateGenEssentialStats(Helpers.timerAndMemory(s, "# Generate Essentials"));
        Logger.println("# Specialized Essential Templates: " + f.format(specializedRules.size()) + " | " +
                "Generated Essential Rules: " + f.format(context.getEssentialRules()), 1);
    }

    private static class RuleWriter extends Thread {
        int id;
        ExecutorService service;
        File file;
        BlockingQueue<String> contents;
        boolean append;

        RuleWriter(int id, ExecutorService service, File file, BlockingQueue<String> contents, boolean append) {
            super("EssentialRule-RuleWriter-" + id);
            this.id = id;
            this.service = service;
            this.file = file;
            this.contents = contents;
            this.append = append;
            start();
        }

        @Override
        public void run() {
            try(PrintWriter writer = new PrintWriter(new FileWriter(file, append))) {
                while(!service.isTerminated() || !contents.isEmpty()) {
                    String line = contents.poll();
                    if(line != null) {
                        writer.println(line);
                    }
                }
            } catch (IOException e) {
                e.printStackTrace();
                System.exit(-1);
            }
        }
    }

    private static class CreateBAR implements Runnable {
        Rule base;
        Pair candidate;
        Collection<Long> originals;
        Collection<Long> groundingOriginals;
        GraphDatabaseService graph;
        BlockingQueue<String> tempFileContents;
        BlockingQueue<String> ruleFileContents;
        Context context;
        Collection<Long> validOriginals;

        CreateBAR(Rule base, Pair candidate
                , Collection<Long> originals
                , Collection<Long> validOriginals
                , Collection<Long> groundingOriginals
                , GraphDatabaseService graph
                , BlockingQueue<String> tempFileContents
                , BlockingQueue<String> ruleFileContents
                , Context context) {
            this.base = base;
            this.candidate = candidate;
            this.originals = originals;
            this.groundingOriginals = groundingOriginals;
            this.graph = graph;
            this.tempFileContents = tempFileContents;
            this.ruleFileContents = ruleFileContents;
            this.context = context;
            this.validOriginals = validOriginals;
        }

        @Override
        public void run() {
            DecimalFormat f = new DecimalFormat("####.#####");
            try(Transaction tx = graph.beginTx()) {
                int totalPredictions = 0, support = 0, groundTruth = originals.size()
                        , validTotalPredictions = 0, validPredictions = 0;

                candidate.subName = (String) graph.getNodeById(candidate.subId).getProperty(Settings.NEO4J_IDENTIFIER);
                candidate.objName = (String) graph.getNodeById(candidate.objId).getProperty(Settings.NEO4J_IDENTIFIER);
                Rule rule = new InstantiatedRule(base, candidate);
                for (Long groundingOriginal : groundingOriginals) {
                    totalPredictions++;
                    if(originals.contains(groundingOriginal))
                        support++;
                    else {
                        validTotalPredictions++;
                        if(validOriginals.contains(groundingOriginal))
                            validPredictions++;
                    }
                }
                int pcaTotalPredictions = base.isFromSubject() ? support : totalPredictions;
                rule.setStats(support, totalPredictions, pcaTotalPredictions, groundTruth, validTotalPredictions, validPredictions);
                if(Template.qualityCheck(rule)) {
                    try {
                        context.updateEssentialRules();
                        tempFileContents.put("2" + ","
                                + rule.getHeadAnchoring() + ","
                                + rule.getTailAnchoring() + ","
                                + f.format(rule.getStandardConf()) + ","
                                + f.format(rule.getSmoothedConf()) + ","
                                + f.format(rule.getPcaConf()) + ","
                                + f.format(rule.getApcaConf()) + ","
                                + f.format(rule.getHeadCoverage()) + ","
                                + f.format(rule.getValidPrecision()));
                        ruleFileContents.put(rule.toString() + "\t"
                                + f.format(rule.getQuality()) + "\t"
                                + f.format(rule.getHeadCoverage()) + "\t"
                                + f.format(rule.getValidPrecision()) + "\t"
                                + (int) rule.stats.support + "\t"
                                + (int) rule.stats.totalPredictions);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                        System.exit(-1);
                    }
                }
                tx.success();
            }
        }
    }

    private static class CreateHAR implements Runnable {
        Rule base;
        long anchoring;
        Collection<Long> originals;
        Collection<Long> groundingOriginals;
        GraphDatabaseService graph;
        BlockingQueue<String> tempFileContents;
        BlockingQueue<String> ruleFileContents;
        Context context;
        Collection<Long> validOriginals;

        CreateHAR(Rule base, long anchoring
                , Collection<Long> originals
                , Collection<Long> validOriginals
                , Collection<Long> groundingOriginals
                , GraphDatabaseService graph
                , BlockingQueue<String> tempFileContents
                , BlockingQueue<String> ruleFileContents
                , Context context) {
            this.base = base;
            this.anchoring = anchoring;
            this.originals = originals;
            this.validOriginals = validOriginals;
            this.groundingOriginals = groundingOriginals;
            this.graph = graph;
            this.tempFileContents = tempFileContents;
            this.ruleFileContents = ruleFileContents;
            this.context = context;
        }

        @Override
        public void run() {
            DecimalFormat f = new DecimalFormat("####.#####");
            try(Transaction tx = graph.beginTx()) {
                int totalPredictions = 0, support = 0, groundTruth = originals.size()
                        , validTotalPredictions = 0, validPredictions = 0;
                String headName = (String) graph.getNodeById(anchoring).getProperty(Settings.NEO4J_IDENTIFIER);
                Rule rule = new InstantiatedRule(base, headName, anchoring);
                for (Long groundingOriginal : groundingOriginals) {
                    totalPredictions++;
                    if(originals.contains(groundingOriginal)) {
                        support++;
                    } else {
                        validTotalPredictions++;
                        if(validOriginals.contains(groundingOriginal))
                            validPredictions++;
                    }
                }
                int pcaTotalPredictions = base.isFromSubject() ? support : totalPredictions;
                rule.setStats(support, totalPredictions, pcaTotalPredictions, groundTruth, validTotalPredictions, validPredictions);
                if(Template.qualityCheck(rule)) {
                    try {
                        context.updateEssentialRules();
                        updateBaseStats(rule);
                        tempFileContents.put("0" + ","
                                + rule.getHeadAnchoring() + ","
                                + f.format(rule.getStandardConf()) + ","
                                + f.format(rule.getSmoothedConf()) + ","
                                + f.format(rule.getPcaConf()) + ","
                                + f.format(rule.getApcaConf()) + ","
                                + f.format(rule.getHeadCoverage()) + ","
                                + f.format(rule.getValidPrecision()));
                        ruleFileContents.put(rule.toString() + "\t"
                                + f.format(rule.getQuality()) + "\t"
                                + f.format(rule.getHeadCoverage()) + "\t"
                                + f.format(rule.getValidPrecision()) + "\t"
                                + (int) rule.stats.support + "\t"
                                + (int) rule.stats.totalPredictions);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                        System.exit(-1);
                    }
                }
                tx.success();
            }
        }

        private synchronized void updateBaseStats(Rule rule) {
            base.stats.support += rule.stats.support;
            base.stats.totalPredictions += rule.stats.totalPredictions;
            base.stats.pcaTotalPredictions += rule.stats.pcaTotalPredictions;
            base.stats.groundTruth += rule.stats.groundTruth;
            base.stats.validTotalPredictions += rule.stats.validTotalPredictions;
            base.stats.validPredictions += rule.stats.validPredictions;
        }
    }

    private static boolean trivialCheck(Rule rule, long head, long tail) {
        if(Settings.ALLOW_INS_REVERSE)
            return false;
        return head == tail && rule.length() == 1 && rule.head.predicate.equals(rule.bodyAtoms.get(0).predicate);
    }

    private static Set<Pair> generateBodyGrounding(Rule rule, GraphDatabaseService graph) {
        Set<Pair> groundings = new HashSet<>();
        String predicate = rule.bodyAtoms.get(0).predicate;
        boolean outgoing = rule.bodyAtoms.get(0).direction.equals(Direction.OUTGOING);
        try(Transaction tx = graph.beginTx()) {
            for (Relationship relationship : graph.getAllRelationships()) {
                if(GlobalTimer.stopEssential() || groundings.size() > Settings.LEARN_GROUNDINGS) break;
                if(relationship.getType().name().equals(predicate)) {
                    groundings.add(outgoing ? new Pair(relationship.getStartNodeId(), relationship.getEndNodeId())
                            : new Pair(relationship.getEndNodeId(), relationship.getStartNodeId()));
                }
            }
            tx.success();
        }
        return groundings;
    }


}
