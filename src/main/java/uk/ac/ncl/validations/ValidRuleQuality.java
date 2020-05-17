package uk.ac.ncl.validations;

import uk.ac.ncl.Settings;
import uk.ac.ncl.core.Engine;
import uk.ac.ncl.core.GraphOps;
import ac.uk.ncl.structure.*;
import uk.ac.ncl.structure.*;
import uk.ac.ncl.utils.IO;
import uk.ac.ncl.utils.Logger;
import uk.ac.ncl.utils.MathUtils;
import com.google.common.collect.Multimap;
import com.google.common.collect.MultimapBuilder;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Transaction;

import java.io.File;
import java.text.DecimalFormat;
import java.text.MessageFormat;
import java.util.*;
import java.util.concurrent.*;
import java.util.function.Supplier;

public class ValidRuleQuality extends Engine {

    public ValidRuleQuality(File config, String logName) {
        super(config, logName);
        graphFile = new File(home, "databases/graph.db");
        graph = IO.loadGraph(graphFile);
        ruleIndexHome = new File(out, "index");
        trainFile = new File(home, "data/annotated_train.txt");
        testFile = new File(home, "data/annotated_test.txt");
        validFile = new File(home, "data/annotated_valid.txt");
        populateTargets();
        Logger.println("\n# Rule Quality Experiment: ");
        Logger.println("# Experiment Data: " + out.getName() + " | Quality Measure: " + Settings.QUALITY_MEASURE);
        Logger.println("# Sigmoid: " + Settings.USE_SIGMOID + " | Overfitting Factor: " + Settings.OVERFITTING_FACTOR);
    }

    public void analyzeRuleComposition() {

        long s = System.currentTimeMillis();
        CountedSet<Integer> lengthSet = new CountedSet<>();
        CountedSet<Integer> lengthQualitySet = new CountedSet<>();
        CountedSet<Integer> lengthHQualitySet = new CountedSet<>();

        CountedSet<String> typeSet = new CountedSet<>();
        CountedSet<String> typeQualitySet = new CountedSet<>();
        CountedSet<String> typeHighQualitySet = new CountedSet<>();

        try(Transaction tx = graph.beginTx()) {
            for (String target : targets) {
                for (Rule rule : IO.readRules(target, ruleIndexHome, graph)) {
                    Template template = (Template) rule;
                    if(template.isClosed()) {
                        typeSet.add("CAR");
                        lengthSet.add(template.length());
                        if(template.getQuality() > 0.1 && template.getHeadCoverage() > 0.01) {
                            typeQualitySet.add("CAR");
                            lengthQualitySet.add(template.length());
                            if(template.getQuality() > 0.7) {
                                typeHighQualitySet.add("CAR");
                                lengthHQualitySet.add(template.length());
                            }
                        }
                    } else {
                        for (SimpleInsRule insRule : template.insRules) {
                            String type = insRule.getType() == 0 ? "HAR" : "BAR";
                            typeSet.add(type);
                            lengthSet.add(insRule.length());
                            if(insRule.getQuality() > 0.1 && insRule.getHeadCoverage() > 0.01) {
                                typeQualitySet.add(type);
                                lengthQualitySet.add(insRule.length());
                                if(insRule.getQuality() > 0.7) {
                                    typeHighQualitySet.add(type);
                                    lengthHQualitySet.add(insRule.length());
                                }
                            }
                        }
                    }
                }
            }
            tx.success();
        }

        List<String> contents = new ArrayList<>();
        int sum = 0;
        for (Integer length : lengthSet) {
            contents.add(MessageFormat.format("len={0}: {1}", length, lengthSet.get(length)));
            sum += lengthSet.get(length);
        }
        Logger.println(MessageFormat.format("\n# All Rules: {0}\n# {1}", sum, String.join(" | ", contents)));

        contents = new ArrayList<>();
        for (String type : typeSet) {
            contents.add(MessageFormat.format("{0}: {1}", type, typeSet.get(type)));
        }
        Logger.println("# " + String.join(" | ", contents));
        
        contents = new ArrayList<>();
        sum = 0;
        for (Integer length : lengthQualitySet) {
            contents.add(MessageFormat.format("len={0}: {1}", length, lengthQualitySet.get(length)));
            sum += lengthQualitySet.get(length);
        }
        Logger.println(MessageFormat.format("\n# Quality Rules: {0}\n# {1}", sum, String.join(" | ", contents)));

        contents = new ArrayList<>();
        for (String type : typeQualitySet) {
            contents.add(MessageFormat.format("{0}: {1}", type, typeQualitySet.get(type)));
        }
        Logger.println("# " + String.join(" | ", contents));

        contents = new ArrayList<>();
        sum = 0;
        for (Integer length : lengthHQualitySet) {
            contents.add(MessageFormat.format("len={0}: {1}", length, lengthHQualitySet.get(length)));
            sum += lengthHQualitySet.get(length);
        }
        Logger.println(MessageFormat.format("\n# High Quality Rules: {0}\n# {1}", sum, String.join(" | ", contents)));

        contents = new ArrayList<>();
        for (String type : typeHighQualitySet) {
            contents.add(MessageFormat.format("{0}: {1}", type, typeHighQualitySet.get(type)));
        }
        Logger.println("# " + String.join(" | ", contents));

        Logger.println("\n# Execution Time: " + ((double) System.currentTimeMillis() - s) / 1000d);
    }

    class Evaluator implements Runnable {
        Rule rule;
        Set<Pair> trainPairs;
        Set<Pair> testPairs;
        BlockingQueue<Rule> queue;
        boolean unsolvable;

        Evaluator(Rule rule, Set<Pair> trainPairs, Set<Pair> testPairs, BlockingQueue<Rule> queue
                , boolean unsolvable) {
            this.rule = rule;
            this.trainPairs = trainPairs;
            this.testPairs = testPairs;
            this.unsolvable = unsolvable;
            this.queue = queue;
        }

        @Override
        public void run() {
            try(Transaction tx = graph.beginTx()) {
                int support = 0, totalPrediction = 0;
                Set<Pair> predictions = createPredictions(rule);
                for (Pair prediction : predictions) {
                    if (!trainPairs.contains(prediction)) {
                        totalPrediction++;
                        if (testPairs.contains(prediction))
                            support++;
                    }
                }
                try {
                    if (totalPrediction != 0) {
                        rule.stats.setPrecision((double) support / totalPrediction);
                    } else {
                        if(unsolvable) {
                            rule.stats.setPrecision(0);
                        }
                    }
                    queue.put(rule);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                    System.exit(-1);
                }
                tx.success();
            }
        }
    }

    public void overfittingEval() {
        int maxTimePerTarget = 3600;
        int topRulePerTargetOverfitting = 6000;
        int skipAt = 100;

        long s = System.currentTimeMillis();
        Logger.println(MessageFormat.format("\n# Overfitting Experiment:" +
                "\n# Max Time per Target = {0} | Top Rule per Target = {1} | Skip@ = {2}\n"
                , maxTimePerTarget, topRulePerTargetOverfitting, skipAt));
        ExecutorService service = Executors.newFixedThreadPool(Settings.THREAD_NUMBER);
        String[] measures = new String[]{"smoothedConf", "standardConf", "pcaConf"};
        Map<String, BlockingQueue<Rule>> withValidRuleMap = new TreeMap<>();
        Map<String, BlockingQueue<Rule>> withoutValidRuleMap = new TreeMap<>();
        for (String measure : measures) {
            withValidRuleMap.put(measure, new LinkedBlockingDeque<>());
            withoutValidRuleMap.put(measure, new LinkedBlockingDeque<>());
        }
        Multimap<Double, Rule> factorMap = MultimapBuilder.treeKeys().arrayListValues().build();
        String factorMeasure = "smoothedConf";
        double originalFactor = Settings.OVERFITTING_FACTOR;

        try(Transaction tx = graph.beginTx()) {
            for (String target : targets) {
                Set<Future<?>> futures = new HashSet<>();
                Set<Pair> trainPairs = IO.readPair(graph, trainFile, target);
                Set<Pair> testPairs = IO.readPair(graph, testFile, target);
                List<Rule> localRules = readRules(target, ruleIndexHome, graph);
                List<Rule> visited = new ArrayList<>();

                long timer = System.currentTimeMillis();
                for (String measure : measures) {
                    double elapsed = ((double) System.currentTimeMillis() - timer) / 1000d;
                    if(elapsed > maxTimePerTarget) {
                        Logger.println("# Skip " + target + " with measure " + measure + " due to target timeout.");
                        continue;
                    }

                    BlockingQueue<Rule> evaluatedRules = new LinkedBlockingDeque<>();
                    localRules.sort(IO.ruleComparatorBySC(measure));
                    for (Rule localRule : localRules.subList(0, Math.min(topRulePerTargetOverfitting, localRules.size()))) {
                        if(visited.contains(localRule)) {
                            evaluatedRules.put(localRule);
                        } else {
                            futures.add(service.submit(new Evaluator(localRule, trainPairs, testPairs, evaluatedRules, true)));
                            visited.add(localRule);
                        }
                    }

                    boolean allDone;
                    do {
                        allDone = true;
                        for (Future<?> future : futures) {
                            if (!future.isDone())
                                allDone = false;
                        }
                        elapsed = ((double) System.currentTimeMillis() - timer) / 1000d;
                    } while (!allDone && !(elapsed > maxTimePerTarget));

                    for (Rule rule : evaluatedRules) {
                        if(!overfitting(rule, measure))
                            withValidRuleMap.get(measure).put(rule);
                        withoutValidRuleMap.get(measure).put(rule);
                    }

                    if(withValidRuleMap.get(measure).size() < skipAt || withoutValidRuleMap.get(measure).size() < skipAt) {
                        Logger.println("# Skip " + target + " with measure " + measure + " due to insufficient amount of rules.");
                        continue;
                    }

                    if(measure.equals(factorMeasure)) {
                        double[] factors = new double[]{0, 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1};
                        for (double factor : factors) {
                            Settings.OVERFITTING_FACTOR = factor;
                            for (Rule rule : evaluatedRules) {
                                if (!overfitting(rule, factorMeasure))
                                    factorMap.put(factor, rule);
                            }
                        }
                        Settings.OVERFITTING_FACTOR = originalFactor;
                    }
                }
            }

            service.shutdown();
            service.awaitTermination(1L, TimeUnit.HOURS);

            Logger.println("\n# Results without validation: ");
            reportOverfittingResult(withoutValidRuleMap);

            Logger.println("# Results with validation: ");
            reportOverfittingResult(withValidRuleMap);

            Logger.println(MessageFormat.format("# Factor Result: Measure: {0} | Type: {1}", factorMeasure, "All"));
            reportFactorOverfitting(factorMap, factorMeasure);

            tx.success();
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(-1);
        }
        Logger.println("\n# Execution Time: " + ((double) System.currentTimeMillis() - s) / 1000d);
    }

    private void reportFactorOverfitting(Multimap<Double, Rule> factorMap, String measure) {
        for (Map.Entry<Double, Collection<Rule>> entry : factorMap.asMap().entrySet()) {
            double factor = entry.getKey();
            double overfittingRules = 0;
            for (Rule rule : entry.getValue()) {
                if(rule.getPrecision() < rule.getQuality(measure) * 0.1)
                    overfittingRules++;
            }
            Logger.println(MessageFormat.format("# Factor = {0}: Overfitting Proportion = {1}"
                    , factor, overfittingRules / entry.getValue().size() ));
        }
    }

    private void reportOverfittingResult(Map<String, BlockingQueue<Rule>> ruleMap) {
        for (Map.Entry<String, BlockingQueue<Rule>> entry : ruleMap.entrySet()) {
            Logger.println(MessageFormat.format("# Measure: {0} | Rule Size: {1}", entry.getKey(), entry.getValue().size()));

            Multimap<String, Rule> allRules = MultimapBuilder.treeKeys().arrayListValues().build();
            Multimap<String, Rule> overfittingRules = MultimapBuilder.treeKeys().arrayListValues().build();
            for (Rule rule : entry.getValue()) {
                if(rule.isClosed()) {
                    allRules.put("CAR", rule);
                    if(rule.getPrecision() < rule.getQuality(entry.getKey()) * 0.1)
                        overfittingRules.put("CAR", rule);
                }
                else {
                    allRules.put("len=" + rule.length(), rule);
                    if(rule.getPrecision() < rule.getQuality(entry.getKey()) * 0.1)
                        overfittingRules.put("len=" + rule.length(), rule);
                }
            }

            double allQuality = 0;
            for (Rule rule : allRules.values()) {
                allQuality += rule.getQuality(entry.getKey());
            }
            Logger.println(MessageFormat.format("# All Rules: Size = {0} | Overfitting Proportion = {1} | Quality = {2}"
                    , allRules.size(), (double) overfittingRules.size() / allRules.size(), allQuality / allRules.size()));

            for (String type : allRules.keySet()) {
                double quality = 0;
                double overfitProportion = (double) overfittingRules.get(type).size() / allRules.size();
                for (Rule rule : allRules.get(type)) {
                    quality += rule.getQuality(entry.getKey());
                }
                quality = quality / allRules.get(type).size();
                Logger.println(MessageFormat.format("# Type {0}: Size = {3} | Overfitting Proportion = {1} | Quality = {2}", type
                        , overfitProportion, quality, allRules.get(type).size()));
            }
            Logger.println("");
        }
    }

    private List<Rule> readRules(String target, File ruleIndexHome, GraphDatabaseService graph) {
        Set<Rule> templates = IO.readRules(target, ruleIndexHome, graph);
        List<Rule> sortedRules = new ArrayList<>();
        for (Rule template : templates) {
            if(template.isClosed())
                sortedRules.add(template);
            else
                sortedRules.addAll(((Template) template).insRules);
        }
        return sortedRules;
    }

    public void evalPrecision() {
        long s = System.currentTimeMillis();
        int reportAt = 50;
        int maxTimePerTarget = 3600;
        int skipAt = 10;
        int topRulePerTargetPrecision = 6000;

        Logger.println(MessageFormat.format("\n# Precision Experiment:" +
                "\n# Precision@ = {0} | Max Time per Target = {1}" +
                "\n# Top Rules per Target = {2} | Skip@ = {3}\n"
                , reportAt
                , maxTimePerTarget
                , topRulePerTargetPrecision
                , skipAt));
        DecimalFormat f = new DecimalFormat("###.#####");
        Multimap<Integer, Double> withValidGlobalPrecisionMap = MultimapBuilder.treeKeys().arrayListValues().build();
        Multimap<Integer, Double> withoutValidGlobalPrecisionMap = MultimapBuilder.treeKeys().arrayListValues().build();
        Multimap<Integer, Double> withValidGlobalQualityMap = MultimapBuilder.treeKeys().arrayListValues().build();
        Multimap<Integer, Double> withoutValidGlobalQualityMap = MultimapBuilder.treeKeys().arrayListValues().build();
        ExecutorService service = Executors.newFixedThreadPool(Settings.THREAD_NUMBER);
        Multimap<Double, Double> factorPrecisionMap = MultimapBuilder.treeKeys().arrayListValues().build();
        Multimap<Double, Double> factorQualityMap = MultimapBuilder.treeKeys().arrayListValues().build();
        double original_factor = Settings.OVERFITTING_FACTOR;

        try(Transaction tx = graph.beginTx()) {
            for (String target : targets) {
                BlockingQueue<Rule> withoutValidEvaluatedRules = new LinkedBlockingDeque<>();
                BlockingQueue<Rule> withValidEvaluatedRules = new LinkedBlockingDeque<>();
                BlockingQueue<Rule> evaluatedRules = new LinkedBlockingDeque<>();
                Set<Future<?>> futures = new HashSet<>();

                Set<Pair> trainPairs = IO.readPair(graph, trainFile, target);
                Set<Pair> testPairs = IO.readPair(graph, testFile, target);
                List<Rule> sortedRules = readRules(target, ruleIndexHome, graph);
                sortedRules.sort(IO.ruleComparatorBySC());

                for (Rule rule : sortedRules.subList(0, Math.min(topRulePerTargetPrecision, sortedRules.size()))) {
                    futures.add(service.submit(new Evaluator(rule, trainPairs, testPairs, evaluatedRules, true)));
                }

                long timer = System.currentTimeMillis();
                boolean allDone;
                while(true) {
                    allDone = true;
                    for (Future<?> future : futures) {
                        if(!future.isDone())
                            allDone = false;
                    }
                    double elapsed = ((double) System.currentTimeMillis() - timer) / 1000d;
                    if(allDone || elapsed > maxTimePerTarget)
                        break;
                }

                for (Rule rule : evaluatedRules) {
                    if(!overfitting(rule))
                        withValidEvaluatedRules.put(rule);
                    withoutValidEvaluatedRules.put(rule);
                }

                if(withValidEvaluatedRules.size() <= skipAt || withoutValidEvaluatedRules.size() <= skipAt) {
                    Logger.println("# Skip " + target + " due to insufficient amount of top rules.");
                    continue;
                }

                Logger.println("# With Validation: ", 3);
                reportPrecision(withValidEvaluatedRules, withValidGlobalPrecisionMap, withValidGlobalQualityMap);
                Logger.println("# Without Validation: ", 3);
                reportPrecision(withoutValidEvaluatedRules, withoutValidGlobalPrecisionMap, withoutValidGlobalQualityMap);

                double[] factors = new double[]{0, 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1};
                for (double factor : factors) {
                    Settings.OVERFITTING_FACTOR = factor;
                    withValidEvaluatedRules.clear();
                    for (Rule rule : evaluatedRules) {
                        if(!overfitting(rule))
                            withValidEvaluatedRules.put(rule);
                    }
                    reportFactorPrecision(withValidEvaluatedRules, factorPrecisionMap, factorQualityMap, reportAt);
                }
                Settings.OVERFITTING_FACTOR = original_factor;
            }

            service.shutdown();
            service.awaitTermination(1L, TimeUnit.HOURS);

            Logger.println("\n# Global Report: ");
            Logger.println("# With Validation: ");
            for (Integer i : withValidGlobalPrecisionMap.keySet()) {
                double precision = MathUtils.listMean(new ArrayList<>(withValidGlobalPrecisionMap.get(i)));
                double quality = MathUtils.listMean(new ArrayList<>(withValidGlobalQualityMap.get(i)));
                Logger.println(MessageFormat.format("# Precision@{0}: {1} | Quality@{0}: {2}"
                        , i, f.format(precision), f.format(quality)));
            }
            Logger.println("");

            Logger.println("# Without Validation: ");
            for (Integer i : withoutValidGlobalPrecisionMap.keySet()) {
                double precision = MathUtils.listMean(new ArrayList<>(withoutValidGlobalPrecisionMap.get(i)));
                double quality = MathUtils.listMean(new ArrayList<>(withoutValidGlobalQualityMap.get(i)));
                Logger.println(MessageFormat.format("# Precision@{0}: {1} | Quality@{0}: {2}"
                        , i, f.format(precision), f.format(quality)));
            }
            Logger.println("");

            for (Double i : factorPrecisionMap.keySet()) {
                double precision = MathUtils.listMean(new ArrayList<>(factorPrecisionMap.get(i)));
                double quality = MathUtils.listMean(new ArrayList<>(factorQualityMap.get(i)));
                Logger.println(MessageFormat.format("# Factor={0}: Precision@{1} = {2} | Quality@{1} = {3}"
                        , i, reportAt, f.format(precision), f.format(quality)));
            }

            tx.success();
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(-1);
        }
        Logger.println("\n# Execution Time: " + ((double) System.currentTimeMillis() - s) / 1000d);
    }

    private void reportFactorPrecision(BlockingQueue<Rule> evaluatedRules, Multimap<Double, Double> precisionMap
            , Multimap<Double, Double> qualityMap, int reportAt) {
        List<Rule> sortedRules = new ArrayList<>(evaluatedRules);
        sortedRules.sort(IO.ruleComparatorBySC());

        int count = 0;
        List<Double> precisions = new ArrayList<>();
        List<Double> qualities = new ArrayList<>();
        for (Rule rule : sortedRules) {
            precisions.add(rule.getPrecision());
            qualities.add(rule.getQuality());
            count++;
        }
        if(reportAt <= count) {
            double precision = MathUtils.listMean(precisions.subList(0, reportAt));
            double quality = MathUtils.listMean(qualities.subList(0, reportAt));
            precisionMap.put(Settings.OVERFITTING_FACTOR, precision);
            qualityMap.put(Settings.OVERFITTING_FACTOR, quality);
        }
    }

    private void reportPrecision(BlockingQueue<Rule> evaluatedRules, Multimap<Integer, Double> precisionMap
            , Multimap<Integer, Double> qualityMap) {
        DecimalFormat f = new DecimalFormat("###.#####");
        List<Rule> sortedRules = new ArrayList<>(evaluatedRules);
        sortedRules.sort(IO.ruleComparatorBySC());

        int count = 0;
        List<Double> precisions = new ArrayList<>();
        List<Double> qualities = new ArrayList<>();
        for (Rule rule : sortedRules) {
            precisions.add(rule.getPrecision());
            qualities.add(rule.getQuality());
            if(count++ < 10) {
                Logger.println("# " + String.join("\t", new String[]{rule.toString()
                        , f.format(rule.getQuality())
                        , f.format(rule.getPrecision())}), 3);
            }
        }

        Logger.println("", 3);
        for (Integer i : Arrays.asList(5, 10, 20, 50, 100)) {
            if(i <= count) {
                double precision = MathUtils.listMean(precisions.subList(0, i));
                double quality = MathUtils.listMean(qualities.subList(0, i));
                Logger.println(MessageFormat.format("# Precision@{0}: {1} | Quality@{0}: {2}"
                        , i, f.format(precision), f.format(quality)), 3);
                precisionMap.put(i, precision);
                qualityMap.put(i, quality);
            }
        }
        Logger.println("", 3);
    }

    public static boolean overfitting(Rule rule) {
        double threshold = Settings.USE_SIGMOID ? MathUtils.sigmoid(rule.length(), rule.getType()) : Settings.OVERFITTING_FACTOR;
        return rule.getQuality() * threshold > rule.getValidPrecision();
    }

    public static boolean overfitting(Rule rule, String measure) {
        double threshold = Settings.USE_SIGMOID ? MathUtils.sigmoid(rule.length(), rule.getType()) : Settings.OVERFITTING_FACTOR;
        return rule.getQuality(measure) * threshold > rule.getValidPrecision();
    }

    public List<Rule> findSolvableRules(Set<Rule> rules, Set<Pair> testPairs) {
        List<Rule> results = new ArrayList<>();
        Set<Long> testSubs = new HashSet<>();
        Set<Long> testObjs = new HashSet<>();
        for (Pair testPair : testPairs) {
            testSubs.add(testPair.subId);
            testObjs.add(testPair.objId);
        }

        for (Rule rule : rules) {
            if(rule.isClosed())
                results.add(rule);
            else {
                Template template = (Template) rule;
                for (SimpleInsRule insRule : template.insRules) {
                    if(insRule.isFromSubject() ? testObjs.contains(insRule.getHeadAnchoring())
                            : testSubs.contains(insRule.getHeadAnchoring()))
                        results.add(insRule);
                }
            }
        }

        results.sort(IO.ruleComparatorBySC());

        return results;
    }

    public Set<Pair> createPredictions(Rule rule) {
        Set<Pair> predictions = new HashSet<>();
        long startTime = System.currentTimeMillis();
        Supplier<Boolean> condition = () -> ((double) System.currentTimeMillis() - startTime) / 1000d > 30;
        CountedSet<Pair> bodyGroundings = GraphOps.bodyGroundingCoreAPI(graph, rule, false, condition);

        if(rule.isClosed()) {
            for (Pair grounding : bodyGroundings)
                predictions.add(rule.isFromSubject() ? grounding : new Pair(grounding.objId, grounding.subId));
        } else {
            Multimap<Long, Long> tailToOriginals = MultimapBuilder.hashKeys().hashSetValues().build();
            for (Pair bodyGrounding : bodyGroundings) {
                tailToOriginals.put(bodyGrounding.objId, bodyGrounding.subId);
            }

            if(rule.getType() == 0) {
                for (Long original : new HashSet<>(tailToOriginals.values())) {
                    predictions.add(rule.isFromSubject() ? new Pair(original, rule.getHeadAnchoring())
                            : new Pair(rule.getHeadAnchoring(), original));
                }
            } else {
                for (Long original : tailToOriginals.get(rule.getTailAnchoring())) {
                    predictions.add(rule.isFromSubject() ? new Pair(original, rule.getHeadAnchoring())
                            : new Pair(rule.getHeadAnchoring(), original));
                }
            }
        }

        return predictions;
    }

}
