package uk.ac.ncl.validations;

import uk.ac.ncl.core.Engine;
import uk.ac.ncl.core.GraphOps;
import ac.uk.ncl.structure.*;
import uk.ac.ncl.structure.*;
import uk.ac.ncl.utils.IO;
import uk.ac.ncl.utils.Logger;
import uk.ac.ncl.utils.MathUtils;
import com.google.common.collect.Multimap;
import com.google.common.collect.MultimapBuilder;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.LineIterator;
import org.neo4j.graphdb.Transaction;

import java.io.File;
import java.text.MessageFormat;
import java.util.*;
import java.util.function.Supplier;

public class ValidRuleEvalEfficiency extends Engine {

    private int sampleSize = 20;
    private int insRuleSize = 1500;
    private int maxTime = 1800;

    public ValidRuleEvalEfficiency(File config, String logName) {
        super(config, logName);
        graphFile = new File(home, "databases/graph.db");
        graph = IO.loadGraph(graphFile);
        ruleIndexHome = new File(out, "index");
        trainFile = new File(home, "data/annotated_train.txt");
        populateTargets();
    }

    /**
     * Constraints:
     * - For each type (CAR, len1, len2, len3), we sample 10 rules with at most 1000 associated ins rules.
     * - When grounding rules, it takes at most 30s.
     * - Each type is allowed at most 30mins to run.
     */
    public void eval() {
        Logger.println(MessageFormat.format("\n# Evaluation Efficiency Experiment:\n" +
                "# Template Size = {0} | Ins Rule Size = {1} | Max Time per method = {2}\n"
                , sampleSize, insRuleSize, maxTime));

        long s = System.currentTimeMillis();
        Multimap<String, Double> sinGlobalRuntime = MultimapBuilder.treeKeys().arrayListValues().build();
        Multimap<String, Double> colGlobalRuntime = MultimapBuilder.treeKeys().arrayListValues().build();

        try (Transaction tx = graph.beginTx()) {
            for (String target : targets) {
                double allSinTime = 0, allColTime = 0;
                Set<Rule> rules = readRules(target);
                Set<Pair> trainPairs = IO.readPair(graph, trainFile, target);

                Multimap<String, Rule> categories = MultimapBuilder.treeKeys().arrayListValues().build();
                for (Rule rule : rules) {
                    if(rule.isClosed())
                        categories.put("CAR", rule);
                    categories.put("len=" + rule.length(), rule);
                }

                categories = sampleRules(categories);
                List<Rule> warmUpSample = new ArrayList<>(categories.get("CAR"));
                warmUp(warmUpSample.subList(0, Math.min(warmUpSample.size(), 3)));

                double sinTime = 0, colTime = 0;
                for (Map.Entry<String, Collection<Rule>> category : categories.asMap().entrySet()) {
                    Logger.println("# Evaluate over " + category.getKey(), 3);
                    sinTime = singleEval(category.getValue(), trainPairs);
                    sinGlobalRuntime.put(category.getKey(), sinTime);
                    allSinTime += sinTime;

                    colTime = collectiveEval(category.getValue(), trainPairs);
                    colGlobalRuntime.put(category.getKey(), colTime);
                    allColTime += colTime;
                    Logger.println("", 3);
                }

                Logger.println("# Evaluate over All\n# Elapsed Time: Single = " + allSinTime + " | Collective = " + allColTime, 3);
                sinGlobalRuntime.put("All", allSinTime);
                colGlobalRuntime.put("All", allColTime);
            }
            tx.success();
        }

        Logger.println("\n# Averaged Results over Targets:");
        for (Map.Entry<String, Collection<Double>> entry : sinGlobalRuntime.asMap().entrySet()) {
            double avgSinTime = MathUtils.listMean(new ArrayList<>(entry.getValue()));
            double avgColTime = MathUtils.listMean(new ArrayList<>(colGlobalRuntime.get(entry.getKey())));
            Logger.println(MessageFormat.format("# {0}: Single = {1} | Collective = {2}"
                    , entry.getKey(), avgSinTime, avgColTime));
        }
        Logger.println("\n# Execution Time: " + ((double) System.currentTimeMillis() - s) / 1000d);
    }

    public Multimap<String, Rule> sampleRules(Multimap<String, Rule> categories) {
        Multimap<String, Rule> sampled = MultimapBuilder.treeKeys().arrayListValues().build();
        for (Map.Entry<String, Collection<Rule>> entry : categories.asMap().entrySet()) {
            List<Rule> rules = new ArrayList<>();
            for (Rule rule : entry.getValue()) {
                Template template = (Template) rule;
                if(template.isClosed())
                    rules.add(rule);
                else {
                    if(template.insRules.size() < insRuleSize)
                        rules.add(rule);
                }
            }

            Collections.shuffle(rules);
            sampled.putAll(entry.getKey(), rules.subList(0, Math.min(rules.size(), sampleSize)));
        }
        return sampled;
    }

    public Set<Rule> readRules(String target) {
        Set<Rule> rules = new HashSet<>();
        int ruleCount = 0;
        File ruleIndexFile = new File(ruleIndexHome, target.replaceAll("[:/<>]", "_") + ".txt");
        try (LineIterator l = FileUtils.lineIterator(ruleIndexFile)) {
            while(l.hasNext()) {
                String line = l.nextLine();
                if(line.startsWith("ABS: ")) {
                    Template rule = new Template(line.split("ABS: ")[1]);
                    if(!rule.isClosed()) {
                        String insRuleLine = l.nextLine();
                        for (String s : insRuleLine.split("\t")) {
                            SimpleInsRule insRule = new SimpleInsRule(rule, s);
                            insRule.insRuleString(graph);
                            rule.insRules.add(insRule);
                            ruleCount++;
                        }
                    } else {
                        String[] words = line.split("ABS: ")[1].split("\t");
                        rule.stats.setStandardConf(Double.parseDouble(words[3]));
                        rule.stats.setSmoothedConf(Double.parseDouble(words[4]));
                        rule.stats.setPcaConf(Double.parseDouble(words[5]));
                        rule.stats.setApcaConf(Double.parseDouble(words[6]));
                        rule.stats.setHeadCoverage(Double.parseDouble(words[7]));
                        ruleCount++;
                    }
                    rules.add(rule);
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(-1);
        }

        Logger.println(MessageFormat.format("# Start Analyzing for Target: {0} | Rule Size: {1}", target, ruleCount));
        return rules;
    }

    public double collectiveEval(Collection<Rule> templates, Set<Pair> groundTruth) {
        long s = System.currentTimeMillis();
        Multimap<Long, Long> subToOriginals = MultimapBuilder.hashKeys().hashSetValues().build();
        Multimap<Long, Long> objToOriginals = MultimapBuilder.hashKeys().hashSetValues().build();

        for (Pair pair : groundTruth) {
            subToOriginals.put(pair.subId, pair.objId);
            objToOriginals.put(pair.objId, pair.subId);
        }

        int count = 0;
        for (Rule rule : templates) {
            double elapsed = ((double) System.currentTimeMillis() - s) / 1000d;
            if(elapsed > maxTime)
                break;

            long startingTime = System.currentTimeMillis();
            Supplier<Boolean> condition = () -> (System.currentTimeMillis() - startingTime) / 1000 > 30;
            CountedSet<Pair> bodyGroundings = GraphOps.bodyGroundingCoreAPI(graph, rule, false, condition);

            Multimap<Long, Long> tailToOriginals = MultimapBuilder.hashKeys().hashSetValues().build();
            for (Pair bodyGrounding : bodyGroundings) {
                tailToOriginals.put(bodyGrounding.objId, bodyGrounding.subId);
            }

            if(rule.isClosed()) {
                count++;
                evalClosedRule(rule, bodyGroundings, groundTruth);
            }
            else {
                Template template = (Template) rule;
                for (SimpleInsRule insRule : template.insRules) {
                    elapsed = ((double) System.currentTimeMillis() - s) / 1000d;
                    if(elapsed > maxTime)
                        break;

                    count++;
                    Collection<Long> originals = insRule.isFromSubject() ? objToOriginals.get(insRule.getHeadAnchoring()) :
                            subToOriginals.get(insRule.getHeadAnchoring());
                    if(insRule.getType() == 0) {
                        evaluateRule(insRule, originals, new HashSet<>(tailToOriginals.values()));
                    } else if(insRule.getType() == 2) {
                        evaluateRule(insRule, originals, tailToOriginals.get(insRule.getTailAnchoring()));
                    }
                }
            }
        }

        Logger.println("# Collective Evaluated Rules: " + count,3);
        double elapsedTime = ((double) System.currentTimeMillis() - s) / 1000d;
        Logger.println("# Elapsed Time: " + elapsedTime + " | Rule/s: " + (int) (count / elapsedTime), 3);
        return elapsedTime;
    }

    public void warmUp(Collection<Rule> rules) {
        long startingTime = System.currentTimeMillis();
        Supplier<Boolean> condition = () -> (System.currentTimeMillis() - startingTime) / 1000 > 1;
        for (Rule rule : rules) {
            GraphOps.bodyGroundingCoreAPI(graph, rule, false, condition);
        }
    }

    public double singleEval(Collection<Rule> templates, Set<Pair> groundTruth) {
        final long s = System.currentTimeMillis();
        Multimap<Long, Long> subToOriginals = MultimapBuilder.hashKeys().hashSetValues().build();
        Multimap<Long, Long> objToOriginals = MultimapBuilder.hashKeys().hashSetValues().build();

        for (Pair pair : groundTruth) {
            subToOriginals.put(pair.subId, pair.objId);
            objToOriginals.put(pair.objId, pair.subId);
        }

        List<Rule> rules = new ArrayList<>();
        for (Rule template : templates) {
            if(template.isClosed())
                rules.add(template);
            else
                rules.addAll(((Template) template).insRules);
        }

        int count = 0;
        for (Rule rule : rules) {
            double elapsed = ((double) System.currentTimeMillis() - s) / 1000d;
            if(elapsed > maxTime)
                break;

            long startingTime = System.currentTimeMillis();
            Supplier<Boolean> condition = () -> (System.currentTimeMillis() - startingTime) / 1000 > 30;
            CountedSet<Pair> bodyGroundings = GraphOps.bodyGroundingCoreAPI(graph, rule, false, condition);

            if(rule.isClosed()) {
                count++;
                evalClosedRule(rule, bodyGroundings, groundTruth);
            }
            else {
                count++;
                Multimap<Long, Long> tailToOriginals = MultimapBuilder.hashKeys().hashSetValues().build();
                for (Pair bodyGrounding : bodyGroundings) {
                    tailToOriginals.put(bodyGrounding.objId, bodyGrounding.subId);
                }

                Collection<Long> originals = rule.isFromSubject() ? objToOriginals.get(rule.getHeadAnchoring()) :
                        subToOriginals.get(rule.getHeadAnchoring());
                if(rule.getType() == 0) {
                    evaluateRule(rule, originals, new HashSet<>(tailToOriginals.values()));
                } else if(rule.getType() == 2) {
                    evaluateRule(rule, originals, tailToOriginals.get(rule.getTailAnchoring()));
                }
            }
        }

        Logger.println("# Single Evaluated Rules: " + count, 3);
        double elapsedTime = ((double) System.currentTimeMillis() - s) / 1000d;
        Logger.println("# Elapsed Time: " + elapsedTime + " | Rule/s: " + (int) (count / elapsedTime), 3);
        return elapsedTime;
    }

    public void evalClosedRule(Rule rule, CountedSet<Pair> bodyGroundings, Set<Pair> groundTruth) {
        double totalPrediction = 0, correctPrediction = 0;
        for (Pair grounding : bodyGroundings) {
            Pair prediction = rule.isFromSubject() ? grounding : new Pair(grounding.objId, grounding.subId);
            if(groundTruth.contains(prediction))
                correctPrediction++;
            totalPrediction++;
        }
        double knownSC = rule.getQuality();
        rule.setStats(correctPrediction, totalPrediction, groundTruth.size());
//        if(knownSC - rule.getQuality() >= 0.00002) {
//            System.err.println("# Error:\n# Rule: " + rule + "\n# KnownSC=" + knownSC + " | Measured= " + rule.getQuality());
//            System.exit(-1);
//        }
    }

    private void evaluateRule(Rule rule, Collection<Long> originals, Collection<Long> groundingOriginals) {
        int totalPrediction = 0, support = 0, groundTruth = originals.size();
        for (Long groundingOriginal : groundingOriginals) {
            if(originals.contains(groundingOriginal)) support++;
            totalPrediction++;
        }
        double knownSC = rule.getQuality();
        rule.setStats(support, totalPrediction, groundTruth);
//        if(knownSC - rule.getQuality() >= 0.00002) {
//            System.err.println("# Error:\n# Rule: " + rule + "\n# KnownSC=" + knownSC + " | Measured= " + rule.getQuality());
//            System.exit(-1);
//        }
    }

}
