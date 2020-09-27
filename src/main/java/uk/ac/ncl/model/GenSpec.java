package uk.ac.ncl.model;

import com.google.common.collect.BiMap;
import com.google.common.collect.HashBiMap;
import com.google.common.collect.Multimap;
import com.google.common.collect.MultimapBuilder;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.LineIterator;
import org.neo4j.graphdb.*;
import uk.ac.ncl.Settings;
import uk.ac.ncl.core.*;
import uk.ac.ncl.graph.InMemoryGraph;
import uk.ac.ncl.structure.*;
import uk.ac.ncl.structure.Package;
import uk.ac.ncl.utils.Helpers;
import uk.ac.ncl.utils.IO;
import uk.ac.ncl.utils.Logger;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.text.DecimalFormat;
import java.text.MessageFormat;
import java.text.NumberFormat;
import java.util.*;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingDeque;

public class GenSpec extends Engine {

    public GenSpec(File config) {
        super(config, "log");
        Settings.VERSION = "public-gs-0.2";
        Settings.DATE = "Sep-2020";
        Logger.println("# Graph Path Feature Learning (GPFL) System\n" +
                "# Version: " + Settings.VERSION +  " | Date: " + Settings.DATE, 1);
        Logger.println(MessageFormat.format("# Cores: {0} | JVM RAM: {1}GB | Physical RAM: {2}GB"
                , runtime.availableProcessors()
                , Helpers.JVMRam()
                , Helpers.systemRAM()), 1);

        Settings.COVER_REPEATS = Helpers.readSetting(args, "cover_repeat", Settings.COVER_REPEATS);
        Settings.TOP_RULES = Helpers.readSetting(args, "top_rules", Settings.TOP_RULES);
        reportSettings();
    }

    private void reportSettings() {
        String msg = MessageFormat.format("\n# Settings:\n" +
                        "# Ins Length = {0} | CAR Length = {1}\n" +
                        "# Support = {2} | Confidence = {3}\n" +
                        "# Head Coverage = {4} | Learn Groundings = {5}\n" +
                        "# Top Rules = {6} | Saturation = {7}\n" +
                        "# Batch Size = {8} | Spec Time = {9}\n" +
                        "# Neo4J Identifier = {10} | Confidence Offset = {11}\n" +
                        "# Overfitting Factor = {12} | Threads = {13}\n" +
                        "# Random Walkers = {14} | Gen Time = {15}\n" +
                        "# Quality Measure = {16}"
                , Settings.INS_DEPTH
                , Settings.CAR_DEPTH
                , Settings.SUPPORT
                , String.valueOf(Settings.CONF)
                , Settings.HEAD_COVERAGE
                , Settings.LEARN_GROUNDINGS == Integer.MAX_VALUE ? "Max" : Settings.LEARN_GROUNDINGS
                , Settings.TOP_RULES
                , Settings.SATURATION
                , Settings.BATCH_SIZE
                , Settings.SPEC_TIME == Integer.MAX_VALUE ? "Max" : Settings.SPEC_TIME
                , Settings.NEO4J_IDENTIFIER
                , Settings.CONFIDENCE_OFFSET
                , Settings.OVERFITTING_FACTOR
                , Settings.THREAD_NUMBER
                , Settings.RANDOM_WALKERS
                , Settings.GEN_TIME == Integer.MAX_VALUE ? "Max" : Settings.GEN_TIME
                , Settings.QUALITY_MEASURE
        );
        Logger.println(msg, 1);
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

    public void buildGraph() {
        graph = IO.loadGraph(new File(home, "databases/graph.db"));
        trainFile = new File(home, "data/train.txt");
        validFile = new File(home, "data/valid.txt");
        testFile = new File(home, "data/test.txt");
//        ruleFile = IO.createEmptyFile(new File(out, "rules.txt"));
        predictionFile = IO.createEmptyFile(new File(out, "predictions.txt"));
        verificationFile = IO.createEmptyFile(new File(out, "verifications.txt"));
        indexing();

        populateTargets();
        int range = Math.max(Settings.INS_DEPTH, Settings.CAR_DEPTH);
        GlobalTimer.programStartTime = System.currentTimeMillis();

        for (String target : targets) {
            Settings.TARGET = target;
            Context context = new Context();

            Logger.println(MessageFormat.format("\n# ({0}\\{1}) Start Applying Rules for Target: {2}",
                    globalTargetCounter++, targets.size(), target), 1);

            Set<Pair> trainPairs = IO.readExamples(trainFile, nodeIndex);
            Set<Pair> validPairs = IO.readExamples(validFile, nodeIndex);
            Set<Pair> testPairs = IO.readExamples(testFile, nodeIndex);
            TripleSet tripleSet = new TripleSet(trainPairs, validPairs, testPairs);

            Logger.println(MessageFormat.format("# Train Size: {0} | Valid Size: {1} | Test Size: {2}"
                    , trainPairs.size(), validPairs.size(), testPairs.size()), 1);

            InMemoryGraph inMemoryGraph = new InMemoryGraph(graph, tripleSet, range);



//            inMemoryGraph.ruleApplication(rules);
            writeQueries(tripleSet);
        }

        Logger.println("");
        score();
    }

    public void run() {
        graph = IO.loadGraph(new File(home, "databases/graph.db"));
        trainFile = new File(home, "data/train.txt");
        validFile = new File(home, "data/valid.txt");
        testFile = new File(home, "data/test.txt");
        ruleFile = IO.createEmptyFile(new File(out, "rules.txt"));
        predictionFile = IO.createEmptyFile(new File(out, "predictions.txt"));
        verificationFile = IO.createEmptyFile(new File(out, "verifications.txt"));
        indexing();

        populateTargets();
        GlobalTimer.programStartTime = System.currentTimeMillis();
        int range = Math.max(Settings.INS_DEPTH, Settings.CAR_DEPTH);

        for (String target : targets) {
            Settings.TARGET = target;
            Context context = new Context();
            Logger.println(MessageFormat.format("\n# ({0}\\{1}) Start Learning Rules for Target: {2}",
                    globalTargetCounter++, targets.size(), target), 1);

            Set<Pair> trainPairs = IO.readExamples(trainFile, nodeIndex);
            Set<Pair> validPairs = IO.readExamples(validFile, nodeIndex);
            Set<Pair> testPairs = IO.readExamples(testFile, nodeIndex);
            TripleSet tripleSet = new TripleSet(trainPairs, validPairs, testPairs);

            Logger.println(MessageFormat.format("# Train Size: {0} | Valid Size: {1} | Test Size: {2}"
                    , trainPairs.size(), validPairs.size(), testPairs.size()), 1);

            GraphOps.removeRelationships(validPairs, graph);
            GraphOps.removeRelationships(testPairs, graph);

            generalization(trainPairs, context);
            specialization(context, trainPairs, validPairs);

            IO.writeRules(ruleFile, context.topRules);

            List<Rule> rules = new ArrayList<>(context.topRules);
            InMemoryGraph inMemoryGraph = new InMemoryGraph(graph, tripleSet, range);
            inMemoryGraph.ruleApplication(rules);

//            ruleApplication(tripleSet, rules);
            writeQueries(tripleSet);

//            GraphOps.addRelationships(validPairs, graph);
//            GraphOps.addRelationships(testPairs, graph);
        }

        Logger.println("");
        score();
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

            generalization(trainPairs, context);
            specialization(context, trainPairs, validPairs);

            IO.writeRules(ruleFile, context.topRules);

//            GraphOps.addRelationships(validPairs, graph);
//            GraphOps.addRelationships(testPairs, graph);
        }
    }

    public void apply() {
        graph = IO.loadGraph(new File(home, "databases/graph.db"));
        trainFile = new File(home, "data/train.txt");
        validFile = new File(home, "data/valid.txt");
        testFile = new File(home, "data/test.txt");
        ruleFile = new File(out, "rules.txt");
        predictionFile = IO.createEmptyFile(new File(out, "predictions.txt"));
        verificationFile = IO.createEmptyFile(new File(out, "verifications.txt"));
        indexing();

        targets = IO.readTargetsFromRules(ruleFile, nodeIndex);
        for (String target : targets) {
            Settings.TARGET = target;
            List<Rule> rules = IO.readRules(ruleFile, nodeIndex, target);

            Logger.println(MessageFormat.format("\n# ({0}\\{1}) Start Applying Rules for Target: {2}",
                    globalTargetCounter++, targets.size(), target), 1);

            Set<Pair> trainPairs = IO.readExamples(trainFile, nodeIndex);
            Set<Pair> validPairs = IO.readExamples(validFile, nodeIndex);
            Set<Pair> testPairs = IO.readExamples(testFile, nodeIndex);
            TripleSet tripleSet = new TripleSet(trainPairs, validPairs, testPairs);

            Logger.println(MessageFormat.format("# Train Size: {0} | Valid Size: {1} | Test Size: {2}"
                    , trainPairs.size(), validPairs.size(), testPairs.size()), 1);

            GraphOps.removeRelationships(validPairs, graph);
            GraphOps.removeRelationships(testPairs, graph);

            ruleApplication(tripleSet, rules);
            writeQueries(tripleSet);

//            GraphOps.addRelationships(validPairs, graph);
//            GraphOps.addRelationships(testPairs, graph);
        }

        Logger.println("");
        score();
    }

    public void score() {
        predictionFile = new File(out, "predictions.txt");

        Multimap<String, Integer> rankMap = MultimapBuilder.hashKeys().arrayListValues().build();
        Multimap<String, Integer> headMap = MultimapBuilder.hashKeys().arrayListValues().build();
        Multimap<String, Integer> tailMap = MultimapBuilder.hashKeys().arrayListValues().build();

        try(LineIterator l = FileUtils.lineIterator(predictionFile)) {
            while(l.hasNext()) {
                String line = l.nextLine();
                boolean headQuery = line.startsWith("Head Query: ");
                Triple testTriple = headQuery ? new Triple(line.split("Head Query: ")[1], 1) :
                        new Triple(line.split("Tail Query: ")[1], 1);
                List<Triple> currentAnswers = new ArrayList<>();
                while(l.hasNext()) {
                    String predictionLine = l.nextLine();
                    if(!predictionLine.equals("")) {
                        currentAnswers.add(new Triple(predictionLine.split("\t")[0], 1));
                    }
                    else break;
                }
                int rank = 0;
                for (Triple currentAnswer : currentAnswers) {
                    if(currentAnswer.equals(testTriple)) {
                        rank = currentAnswers.indexOf(currentAnswer) + 1;
                        break;
                    }
                }
                rankMap.put(testTriple.pred, rank);
                if(headQuery)
                    headMap.put(testTriple.pred, rank);
                else
                    tailMap.put(testTriple.pred, rank);
            }
        } catch (IOException e) {
            e.printStackTrace();
            System.exit(-1);
        }

        reportResults(rankMap, headMap, tailMap);
    }

    static private void reportResults(Multimap<String, Integer> rankMap,
                                      Multimap<String, Integer> headMap,
                                      Multimap<String, Integer> tailMap) {
        Multimap<String, Double> perPredicateResults = MultimapBuilder.hashKeys().arrayListValues().build();
        for (String predicate : rankMap.keySet()) {
            Evaluator.printResultsSinglePredicate(predicate, new ArrayList<>(headMap.get(predicate))
                    , new ArrayList<>(tailMap.get(predicate))
                    , new ArrayList<>(rankMap.get(predicate))
                    , 3, perPredicateResults, true);
        }
        switch (Settings.EVAL_PROTOCOL) {
            case "TransE":
                Evaluator.printResultsSinglePredicate("TransE Protocol - All Targets", new ArrayList<>(headMap.values())
                        , new ArrayList<>(tailMap.values())
                        , new ArrayList<>(rankMap.values())
                        , 1, perPredicateResults, false);
                break;
            case "GPFL":
                Evaluator.printAverageResults(perPredicateResults);
                break;
            case "All":
                Evaluator.printResultsSinglePredicate("TransE Protocol - All Targets", new ArrayList<>(headMap.values())
                        , new ArrayList<>(tailMap.values())
                        , new ArrayList<>(rankMap.values())
                        , 1, perPredicateResults, false);
                Evaluator.printAverageResults(perPredicateResults);
                break;
            default:
                System.err.println("# Unknown Evaluation Protocol is selected.");
                System.exit(-1);
        }
    }

    public void writeQueries(TripleSet tripleSet) {
        DecimalFormat f = new DecimalFormat("##.####");
        try(PrintWriter predictionWriter = new PrintWriter(new FileWriter(predictionFile, true))) {
            try(PrintWriter verificationWriter = new PrintWriter(new FileWriter(verificationFile, true))) {
                for (TestQuery q : tripleSet.testQueries) {
                    String content = (q.headQuery ? "Head Query: " : "Tail Query: ") + toQueryString(q.testPair) + "\n";
                    for (Pair answer : q.getTopPairs(Settings.TOP_K)) {
                        String score = f.format(q.getSuggestingRules(answer).get(0).getQuality(Settings.QUALITY_MEASURE));
                        content += toQueryString(answer) + "\t" + score + "\n";
                    }
                    content += "\n";
                    predictionWriter.print(content);
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
            System.exit(-1);
        }
    }

    private String toQueryString(Pair pair) {
        return MessageFormat.format("({0}|{1}, {2}, {3}|{4})",
                String.valueOf(pair.subId),
                nodeIndex.inverse().get(pair.subId),
                Settings.TARGET,
                String.valueOf(pair.objId),
                nodeIndex.inverse().get(pair.objId));
    }

    public void ruleApplication(TripleSet tripleSet, List<Rule> rules) {
        long s = System.currentTimeMillis();

        LinkedBlockingDeque<Package> inputQueue = new LinkedBlockingDeque<>();
        Package.resetIndex();
        rules.forEach(r -> inputQueue.add(Package.create(r)));
        ConcurrentHashMap<Integer, Package> outputQueue = new ConcurrentHashMap<>();

        Dispatcher dispatcher = new Dispatcher(inputQueue, outputQueue, tripleSet);
        RuleApplier[] appliers = new RuleApplier[Settings.THREAD_NUMBER];
        for (int i = 0; i < appliers.length; i++) {
            appliers[i] = new RuleApplier(i, dispatcher, outputQueue, inputQueue, graph, tripleSet);
        }
        try {
            for (RuleApplier applier : appliers) {
                applier.join();
            }
            dispatcher.join();
        } catch (InterruptedException e) {
            e.printStackTrace();
            System.exit(-1);
        }

        GlobalTimer.updateRuleApplyStats(Helpers.timerAndMemory(s,"# Rule Application"));
    }

    static class RuleApplier extends Thread {
        Dispatcher dispatcher;
        ConcurrentHashMap<Integer, Package> outputQueue;
        LinkedBlockingDeque<Package> inputQueue;
        GraphDatabaseService graph;
        TripleSet tripleSet;

        public RuleApplier(int id, Dispatcher dispatcher
                , ConcurrentHashMap<Integer, Package> outputQueue
                , LinkedBlockingDeque<Package> inputQueue
                , GraphDatabaseService graph
                , TripleSet tripleSet) {
            super("RuleApplier-" + id);
            this.dispatcher = dispatcher;
            this.outputQueue = outputQueue;
            this.inputQueue = inputQueue;
            this.graph = graph;
            this.tripleSet = tripleSet;
            start();
        }

        @Override
        public void run() {
            try(Transaction tx = graph.beginTx()) {
                Thread.sleep(500);
                while (dispatcher.isAlive() && !inputQueue.isEmpty()) {
                    if (outputQueue.size() < 3000) {
                        Package p = inputQueue.poll();
                        if (p != null) {
                            p.candidates = groundRules(p.rule);
                            outputQueue.put(p.id, p);
                        }
                    }
                }
                tx.success();
            } catch (InterruptedException e) {
                e.printStackTrace();
                System.exit(-1);
            }
        }

        private Set<Pair> groundRules(Rule pattern) {
            Set<Pair> pairs = new HashSet<>();
            Flag stop = new Flag();

            boolean checkTail = false;
            if(pattern instanceof InstantiatedRule) {
                int type = pattern.getType();
                if(type == 1 || type == 2) checkTail = true;
            }

            Set<Relationship> currentRelationships = GraphOps.getRelationshipsAPI(graph, pattern.getBodyAtom(0).getBasePredicate());
            for (Relationship relationship : currentRelationships) {
                if(stop.flag) break;

                LocalPath currentPath = new LocalPath(relationship, pattern.getBodyAtom(0).direction);
                DFSGrounding(pattern, currentPath, pairs, stop, checkTail);
            }

            return pairs;
        }

        private void DFSGrounding(Rule pattern, LocalPath path
                , Set<Pair> pairs
                , Flag stop
                , boolean checkTail) {
            if(path.length() == pattern.length()) {
                Pair current;
                if(pattern.closed) {
                    current = pattern.isFromSubject() ?
                            new Pair(path.getStartNode().getId(), path.getEndNode().getId()) :
                            new Pair(path.getEndNode().getId(), path.getStartNode().getId());
                } else {
                    InstantiatedRule insPattern = (InstantiatedRule) pattern;
                    if(checkTail) {
                        long currentTail = path.getEndNode().getId();
                        if(insPattern.getTailAnchoring() != currentTail) return;
                    }

                    long currentOriginal = path.getStartNode().getId();
                    current = pattern.isFromSubject() ? new Pair(currentOriginal, pattern.getHeadAnchoring()) :
                            new Pair(pattern.getHeadAnchoring(), currentOriginal);
                }

                if(!tripleSet.inNonTest(current) && tripleSet.possibleSolution(current)) {
                    pairs.add(current);
                }
            }
            else {
                Direction nextDirection = pattern.getBodyAtom(path.length()).direction;
                RelationshipType nextType = RelationshipType.withName(pattern.getBodyAtom(path.length()).predicate);
                for (Relationship relationship : path.getEndNode().getRelationships(nextDirection, nextType)) {
                    if(!path.nodes.contains(relationship.getOtherNode(path.getEndNode()))) {
                        LocalPath currentPath = new LocalPath(path, relationship);
                        DFSGrounding(pattern, currentPath, pairs, stop, checkTail);
                        if (stop.flag) break;
                    }
                }
            }
        }
    }

    static class Dispatcher extends Thread {
        LinkedBlockingDeque<Package> inputQueue;
        ConcurrentHashMap<Integer, Package> outputQueue;
        TripleSet tripleSet;
        int current = 0;

        public Dispatcher(LinkedBlockingDeque<Package> inputQueue
                ,ConcurrentHashMap<Integer, Package> outputQueue, TripleSet tripleSet) {
            this.outputQueue = outputQueue;
            this.tripleSet = tripleSet;
            this.inputQueue = inputQueue;
            start();
        }

        @Override
        public void run() {
            DecimalFormat f = new DecimalFormat("###.####");
            int allRules = inputQueue.size();
            int previous = 0;
            boolean converge = false;

            while(true) {
                if(outputQueue.containsKey(current)) {
                    Package p = outputQueue.remove(current);
                    current++;
                    if(!p.candidates.isEmpty())
                        tripleSet.updateTestCases(p);
                        converge = tripleSet.converge();
                }

                if(current % 5000 == 0 && current != previous) {
                    Logger.println("# Visited " + current + " Rules | Coverage: " + f.format(tripleSet.coverage));
                }
                previous = current;

                boolean empty = inputQueue.isEmpty() && outputQueue.isEmpty();
                if(converge) break;
                else if(empty && current == allRules) break;
            }

            Logger.println(MessageFormat.format("# Visited/All Rules: {0}/{1} | Coverage: {2}",
                    current, allRules, f.format(tripleSet.coverage)));
        }
    }

    public static class Flag {
        public boolean flag;
        public Flag() {
            flag = false;
        }
    }

    private void specialization(Context context, Set<Pair> trainPairs, Set<Pair> validPairs) {
        NumberFormat f = NumberFormat.getNumberInstance(Locale.US);
        long s = System.currentTimeMillis();

        Multimap<Long, Long> objOriginalMap = MultimapBuilder.hashKeys().hashSetValues().build();
        Multimap<Long, Long> subOriginalMap = MultimapBuilder.hashKeys().hashSetValues().build();
        for (Pair trainPair : trainPairs) {
            objOriginalMap.put(trainPair.objId, trainPair.subId);
            subOriginalMap.put(trainPair.subId, trainPair.objId);
        }

        Multimap<Long, Long> validObjToSub = MultimapBuilder.hashKeys().hashSetValues().build();
        Multimap<Long, Long> validSubToObj = MultimapBuilder.hashKeys().hashSetValues().build();
        for (Pair validPair : validPairs) {
            validObjToSub.put(validPair.objId, validPair.subId);
            validSubToObj.put(validPair.subId, validPair.objId);
        }

        BlockingQueue<Rule> abstractRuleQueue = new LinkedBlockingDeque<>(context.sortTemplates());
        GlobalTimer.setSpecStartTime(System.currentTimeMillis());

        SpecializationTask[] tasks = new SpecializationTask[Settings.THREAD_NUMBER];
        for (int i = 0; i < tasks.length; i++) {
            tasks[i] = new SpecializationTask(i, graph, abstractRuleQueue, trainPairs, validPairs,
                    objOriginalMap, subOriginalMap, validObjToSub, validSubToObj, context);
        }
        try {
            for (SpecializationTask task : tasks) {
                task.join();
            }
        } catch (InterruptedException e) {
            e.printStackTrace();
            System.exit(-1);
        }

        GlobalTimer.updateInsRuleStats(Helpers.timerAndMemory(s,"# Specialization"));
        Logger.println(Context.analyzeRuleComposition("# Specialized Templates", context.getSpecializedRules()), 1);
        Logger.println("# All Instantiated Rules: " + f.format(context.getTotalInsRules() + context.getEssentialRules()), 1);
    }

    static class SpecializationTask extends Thread {
        int id;
        GraphDatabaseService graph;
        BlockingQueue<Rule> abstractRuleQueue;
        Context context;
        Set<Pair> trainPairs;
        Set<Pair> validPairs;
        Multimap<Long, Long> objOriginalMap;
        Multimap<Long, Long> subOriginalMap;
        Multimap<Long, Long> validObjToSub;
        Multimap<Long, Long> validSubToObj;

        public SpecializationTask(int id
                , GraphDatabaseService graph
                , BlockingQueue<Rule> abstractRuleQueue
                , Set<Pair> trainPairs
                , Set<Pair> validPairs
                , Multimap<Long, Long> objOriginalMap
                , Multimap<Long, Long> subOriginalMap
                , Multimap<Long, Long> validObjToSub
                , Multimap<Long, Long> validSubToObj
                , Context context) {
            super("InstantiationTask-" + id);
            this.id = id;
            this.graph = graph;
            this.abstractRuleQueue = abstractRuleQueue;
            this.trainPairs = trainPairs;
            this.objOriginalMap = objOriginalMap;
            this.subOriginalMap = subOriginalMap;
            this.context = context;
            this.validObjToSub = validObjToSub;
            this.validSubToObj = validSubToObj;
            this.validPairs = validPairs;
            start();
        }

        @Override
        public void run() {
            try(Transaction tx = graph.beginTx()) {
                while (!abstractRuleQueue.isEmpty() && !GlobalTimer.stopSpec()) {
                    Template abstractRule = (Template) abstractRuleQueue.poll();
                    if(abstractRule != null) {
                        Multimap<Long, Long> anchoringToOriginalMap = abstractRule.isFromSubject() ? objOriginalMap : subOriginalMap;
                        Multimap<Long, Long> validOriginals = abstractRule.isFromSubject() ? validObjToSub : validSubToObj;
                        Specialization(abstractRule, graph, trainPairs, validPairs, anchoringToOriginalMap, validOriginals, context);
                    }
                }
                tx.success();
            }
        }

        public void Specialization(Rule rule, GraphDatabaseService graph, Set<Pair> groundTruth, Set<Pair> validPair
                , Multimap<Long, Long> anchoringToOriginal, Multimap<Long, Long> validOriginals
                , Context context) {
            CountedSet<Pair> bodyGroundings = GraphOps.bodyGroundingCoreAPI(graph, rule, false, GlobalTimer::stopSpec);
            if(GlobalTimer.stopSpec()) return;

            if(rule.closed) {
                if(evalClosedRule(rule, bodyGroundings, groundTruth, validPair)) {
                    context.addTopRules(rule);
                    context.addSpecializedRules(rule);
                }
            }
            else {
                boolean valid = false;

                rule.stats.groundTruth = groundTruth.size();
                Multimap<Long, Long> originalToTail = MultimapBuilder.hashKeys().hashSetValues().build();
                Multimap<Long, Long> tailToOriginal = MultimapBuilder.hashKeys().hashSetValues().build();
                for (Pair bodyGrounding : bodyGroundings) {
                    originalToTail.put(bodyGrounding.subId, bodyGrounding.objId);
                    tailToOriginal.put(bodyGrounding.objId, bodyGrounding.subId);
                }
                Set<Long> groundingOriginals = originalToTail.keySet();

                for (Long anchoring : anchoringToOriginal.keySet()) {
                    if(GlobalTimer.stopSpec()) break;

                    Set<Pair> visited = new HashSet<>();
                    String headName = (String) graph.getNodeById(anchoring).getProperty(Settings.NEO4J_IDENTIFIER);
                    Rule HAR = new InstantiatedRule(rule, headName, anchoring);
                    if(evaluateRule(HAR, anchoringToOriginal.get(anchoring), validOriginals.get(anchoring), groundingOriginals)) {
                        valid = true;
                        context.addTopRules(HAR);
                        context.updateTotalInsRules();
                    }

                    for (Long original : anchoringToOriginal.get(anchoring)) {
                        if(GlobalTimer.stopSpec()) break;

                        for (Long tail : originalToTail.get(original)) {
                            if(GlobalTimer.stopSpec()) break;
                            Pair candidate = new Pair(anchoring, tail);
                            if(!visited.contains(candidate) && !trivialCheck(rule, anchoring, tail)) {
                                visited.add(new Pair(anchoring, tail));
                                candidate.subName = headName;
                                candidate.objName = (String) graph.getNodeById(tail).getProperty(Settings.NEO4J_IDENTIFIER);
                                Rule BAR = new InstantiatedRule(rule, candidate);
                                if (evaluateRule(BAR, anchoringToOriginal.get(anchoring), validOriginals.get(anchoring), tailToOriginal.get(tail))) {
                                    valid = true;
                                    context.addTopRules(BAR);
                                    context.updateTotalInsRules();
                                }
                            }
                        }
                    }
                }

                rule.stats.compute();
                if(valid)
                    context.addSpecializedRules(rule);
            }
        }

        private boolean evalClosedRule(Rule rule, CountedSet<Pair> bodyGroundings, Set<Pair> groundTruth, Set<Pair> validPair) {
            double totalPrediction = 0, correctPrediction = 0, pcaTotalPrediction = 0
                    , validTotalPredictions = 0, validPredictions = 0;

            Set<Long> subs = new HashSet<>();
            for (Pair pair : groundTruth)
                subs.add(pair.subId);

            for (Pair grounding : bodyGroundings) {
                long sub = rule.fromSubject ? grounding.subId : grounding.objId;
                if(subs.contains(sub))
                    pcaTotalPrediction++;

                Pair prediction = rule.fromSubject ? grounding : new Pair(grounding.objId, grounding.subId);
                if(groundTruth.contains(prediction))
                    correctPrediction++;
                else {
                    validTotalPredictions++;
                    if(validPair.contains(prediction))
                        validPredictions++;
                }

                totalPrediction++;
            }

            rule.setStats(correctPrediction, totalPrediction, pcaTotalPrediction
                    , groundTruth.size(), validTotalPredictions, validPredictions);

            return qualityCheck(rule);
        }

        private boolean evaluateRule(Rule rule, Collection<Long> originals, Collection<Long> validOriginals, Collection<Long> groundingOriginals) {
            int totalPrediction = 0, support = 0, groundTruth = originals.size()
                    , validTotalPredictions = 0, validPredictions = 0;
            for (Long groundingOriginal : groundingOriginals) {
                totalPrediction++;
                if(originals.contains(groundingOriginal))
                    support++;
                else {
                    validTotalPredictions++;
                    if(validOriginals.contains(groundingOriginal))
                        validPredictions++;
                }
            }
            int pcaTotalPredictions = rule.isFromSubject() ? support : totalPrediction;
            rule.setStats(support, totalPrediction, pcaTotalPredictions, groundTruth, validTotalPredictions, validPredictions);
            return qualityCheck(rule);
        }

        private boolean qualityCheck(Rule rule) {
            return (rule.stats.support >= Settings.SUPPORT)
                    && (rule.getQuality() >= Settings.CONF)
                    && (rule.getHeadCoverage() >= Settings.HEAD_COVERAGE)
                    && (rule.getValidPrecision() >= rule.getQuality() * Settings.OVERFITTING_FACTOR);
        }

        private boolean trivialCheck(Rule rule, long head, long tail) {
            if(Settings.ALLOW_INS_REVERSE)
                return false;
            return head == tail && rule.length() == 1 && rule.head.predicate.equals(rule.bodyAtoms.get(0).predicate);
        }
    }
}
