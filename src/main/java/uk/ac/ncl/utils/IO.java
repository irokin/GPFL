package uk.ac.ncl.utils;

import com.google.common.collect.BiMap;
import org.neo4j.graphdb.*;
import uk.ac.ncl.Settings;
import uk.ac.ncl.core.Context;
import uk.ac.ncl.structure.*;
import com.google.common.collect.Multimap;
import com.google.common.collect.MultimapBuilder;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.LineIterator;
import org.json.JSONArray;
import org.json.JSONObject;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import uk.ac.ncl.structure.*;

import java.io.*;
import java.text.DecimalFormat;
import java.text.MessageFormat;
import java.util.*;
import java.util.function.Predicate;
import java.util.stream.Collectors;

public class IO {
    public static GraphDatabaseService loadGraph(File graphFile) {
        Logger.println("\n# Load Neo4J Graph from: " + graphFile.getPath(), 1);
        GraphDatabaseService graph = new GraphDatabaseFactory()
                .newEmbeddedDatabase( graphFile );
        Runtime.getRuntime().addShutdownHook( new Thread( graph::shutdown ));

        DecimalFormat format = new DecimalFormat("####.###");

        try(Transaction tx = graph.beginTx()) {
            long relationshipTypes = graph.getAllRelationshipTypes().stream().count();
            long relationships = graph.getAllRelationships().stream().count();
            long nodes = graph.getAllNodes().stream().count();

            Logger.println(MessageFormat.format("# Relationship Types: {0} | Relationships: {1} " +
                    "| Nodes: {2} | Instance Density: {3} | Degree: {4}",
                    relationshipTypes,
                    relationships,
                    nodes,
                    format.format((double) relationships / relationshipTypes),
                    format.format((double) relationships / nodes)), 1);
            tx.success();
        }

        return graph;
    }

    public static List<Instance> readInstance(GraphDatabaseService graph, File in) {
        List<Instance> instances = new ArrayList<>();
        try(Transaction tx = graph.beginTx()) {
            try(LineIterator l = FileUtils.lineIterator(in)) {
                while(l.hasNext()) {
                    long relationshipId = Long.parseLong(l.nextLine().split("\t")[0]);
                    instances.add(new Instance(graph.getRelationshipById(relationshipId)));
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
            tx.success();
        }
        Collections.shuffle(instances);
        return instances;
    }

    public static List<Instance> readInstance(GraphDatabaseService graph, File in, String target) {
        List<Instance> instances = new ArrayList<>();
        try(Transaction tx = graph.beginTx()) {
            try(LineIterator l = FileUtils.lineIterator(in)) {
                while(l.hasNext()) {
                    String currentLine = l.nextLine();
                    long relationshipId = Long.parseLong(currentLine.split("\t")[0]);
                    String relationType = currentLine.split("\t")[2];
                    if(relationType.equals(target))
                        instances.add(new Instance(graph.getRelationshipById(relationshipId)));
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
            tx.success();
        }
        Collections.shuffle(instances);
        return instances;
    }

    public static Set<Pair> readPair(GraphDatabaseService graph, File in, String target) {
        Set<Pair> pairs = new HashSet<>();
        try(Transaction tx = graph.beginTx()) {
            try(LineIterator l = FileUtils.lineIterator(in)) {
                while(l.hasNext()) {
                    String[] words = l.nextLine().split("\t");
                    long relationId = Long.parseLong(words[0]);
                    long headId = Long.parseLong(words[1]);
                    String type = words[2];
                    long tailId = Long.parseLong(words[3]);
                    if(type.equals(target)) {
                        if(relationId != -1) {
                            Relationship rel = graph.getRelationshipById(relationId);
                            pairs.add(new Pair(rel.getStartNodeId(), rel.getEndNodeId(), rel.getId()
                                    , rel, rel.getType()
                                    , (String) rel.getStartNode().getProperty(Settings.NEO4J_IDENTIFIER)
                                    , (String) rel.getEndNode().getProperty(Settings.NEO4J_IDENTIFIER)
                                    , rel.getType().name()));
                        } else {
                            pairs.add(new Pair(headId, tailId));
                        }
                    }
                }
            } catch (IOException e) {
                e.printStackTrace();
                System.exit(-1);
            }
            tx.success();
        }
        return pairs;
    }

    public static void writeInstance(GraphDatabaseService graph, File out, List<Instance> instances) {
        try(Transaction tx = graph.beginTx()) {
            try (PrintWriter writer = new PrintWriter(new FileWriter(out))) {
                for (Instance instance : instances) {
                    writer.println(MessageFormat.format("{0}\t{1}\t{2}\t{3}"
                            , String.valueOf(instance.relationship.getId())
                            , instance.startNodeName
                            , instance.type.name()
                            , instance.endNodeName));
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
            tx.success();
        }
    }

    /**
     * @return A reverse order comparator
     */
    public static Comparator<Rule> ruleComparatorBySC() {
        return (o11, o21) -> {
            double v1 = o11.getQuality();
            double v2 = o21.getQuality();
            if( v2 > v1 ) return 1;
            else if ( v1 > v2 ) return -1;
            else return 0;
        };
    }

    public static Comparator<Rule> ruleComparatorBySC(String measure) {
        return (o11, o21) -> {
            double v1 = o11.getQuality(measure);
            double v2 = o21.getQuality(measure);
            if( v2 > v1 ) return 1;
            else if ( v1 > v2 ) return -1;
            else return 0;
        };
    }

    public static void writePredictedFacts(GraphDatabaseService graph, File predictionFile, List<Map<Long, List<Pair>>> rankedMap) {
        try (PrintWriter writer = new PrintWriter(new FileWriter(predictionFile))) {
            for (int i = 0; i < 2; i++) {
                for (Long key : rankedMap.get(i).keySet()) {
                    String header = i == 0 ? "Query: " + Settings.TARGET + "("
                            + graph.getNodeById(key).getProperty(Settings.NEO4J_IDENTIFIER)
                            + ", ?)"
                            : "Query: " + Settings.TARGET + "("
                            + "(?, "
                            + graph.getNodeById(key).getProperty(Settings.NEO4J_IDENTIFIER) + ")";
                    writer.println(header);
                    List<Pair> localPairs = rankedMap.get(i).get(key);
                    localPairs = localPairs.size() > Settings.TOP_K
                            ? localPairs.subList(0, Settings.TOP_K) : localPairs;
                    localPairs.forEach(pair -> {
                        String subName = (String) graph.getNodeById(pair.subId).getProperty(Settings.NEO4J_IDENTIFIER);
                        String objName = (String) graph.getNodeById(pair.objId).getProperty(Settings.NEO4J_IDENTIFIER);
                        writer.println(MessageFormat.format("({0}, {1}, {2})\t{3}"
                                ,subName
                                ,Settings.TARGET
                                ,objName
                                ,pair.scores[0]));
                    });
                    writer.println();
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static File createEmptyFile(File file) {
        try{
            file.createNewFile();
            PrintWriter writer = new PrintWriter(file);
        } catch (IOException e) {
            e.printStackTrace();
            System.exit(-1);
        }
        return file;
    }

    public static Set<String> readTargets(File file) {
        Set<String> targets = new HashSet<>();
        try(LineIterator l = FileUtils.lineIterator(file)) {
            while(l.hasNext()) {
                String[] words = l.nextLine().split("\t");
                if(words.length == 4)
                    targets.add(words[2]);
                else if(words.length == 3)
                    targets.add(words[1]);
                else {
                    System.err.println("# Invalid triples.");
                    System.exit(-1);
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
            System.exit(-1);
        }
        return targets;
    }

    public static int checkFileLength(File file) {
        int count = 0;
        try {
            BufferedReader reader = new BufferedReader(new FileReader(file));
            while(reader.readLine() != null) count++;
            reader.close();
        } catch (IOException e) {
            e.printStackTrace();
            System.exit(-1);
        }
        return count;
    }

    public static Multimap<String, Triple> readTripleMap(File file) {
        Multimap<String, Triple> tripleMap = MultimapBuilder.hashKeys().hashSetValues().build();
        try(LineIterator l = FileUtils.lineIterator(file)) {
            while(l.hasNext()) {
                String line = l.nextLine();
                if (line != null) {
                    String[] words = line.split("\t");
                    tripleMap.put(words[1], new Triple(words[0], words[1], words[2]));
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
            System.exit(-1);
        }
        return tripleMap;
    }

    public static void writeTriples(File file, Set<String> targets, Multimap<String, Triple> tripleMap) {
        try(PrintWriter writer = new PrintWriter(file)) {
            for (String target : targets) {
                for (Triple triple : tripleMap.get(target)) {
                    writer.println(triple.toString());
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
            System.exit(-1);
        }
    }

    public static void writeTriples(File file, Set<Triple> triples) {
        try(PrintWriter writer = new PrintWriter(file)) {
            for (Triple triple : triples) {
                writer.println(triple);
            }
        } catch (IOException e) {
            e.printStackTrace();
            System.exit(-1);
        }
    }

    public static void writeAllTriples(File file, Set<Triple> triples) {
        try(PrintWriter writer = new PrintWriter(file)) {
            for (Triple triple : triples) {
                writer.println(triple);
            }
        } catch (IOException e) {
            e.printStackTrace();
            System.exit(-1);
        }
    }

    public static double[] readSplitRatio(JSONArray array) {
        double[] splitRatio = new double[2];
        assert array.length() == 2;
        for (int i = 0; i < array.length(); i++) {
            splitRatio[i] = array.getDouble(i);
        }
        return splitRatio;
    }

    public static void orderRuleIndexFile(File ruleIndexFile) {
        DecimalFormat f = new DecimalFormat("####.#####");
        Map<String, Double> map = new HashMap<>();
        try(LineIterator l = FileUtils.lineIterator(ruleIndexFile)) {
            while(l.hasNext()) {
                String line = l.nextLine();
                if(line.startsWith("ABS: ")) {
                    String[] words = line.split("ABS: ")[1].split("\t");
                    if(words[1].equals("CAR")) {
                        double score = 0;
                        switch (Settings.QUALITY_MEASURE) {
                            case "standardConf":
                                score = Double.parseDouble(words[3]);
                                break;
                            case "smoothedConf":
                                score = Double.parseDouble(words[4]);
                                break;
                            case "pcaConf":
                                score = Double.parseDouble(words[5]);
                                break;
                            default:
                                score = Double.parseDouble(words[6]);
                        }
                        map.put(line, score);
                    } else {
                        String nextLine = l.nextLine();
                        if(!nextLine.equals("")) {
                            String[] insRules = nextLine.split("\t");
                            List<Double> scores = new ArrayList<>();
                            for (String insRule : insRules) {
                                String[] components = insRule.split(",");
                                if(components[0].equals("0")) {
                                    switch (Settings.QUALITY_MEASURE) {
                                        case "standardConf":
                                            scores.add(Double.parseDouble(components[2]));
                                            break;
                                        case "smoothedConf":
                                            scores.add(Double.parseDouble(components[3]));
                                            break;
                                        case "pcaConf":
                                            scores.add(Double.parseDouble(components[4]));
                                            break;
                                        default:
                                            scores.add(Double.parseDouble(components[5]));
                                    }
                                } else if(components[0].equals("2")) {
                                    switch (Settings.QUALITY_MEASURE) {
                                        case "standardConf":
                                            scores.add(Double.parseDouble(components[3]));
                                            break;
                                        case "smoothedConf":
                                            scores.add(Double.parseDouble(components[4]));
                                            break;
                                        case "pcaConf":
                                            scores.add(Double.parseDouble(components[5]));
                                            break;
                                        default:
                                            scores.add(Double.parseDouble(components[6]));
                                    }
                                } else {
                                    System.err.println("# Error: Unknown rule type in rule index file.");
                                }
                            }
                            double score = MathUtils.listMean(scores);
                            map.put(line + "\t" + f.format(score) + "\n" + nextLine, score);
                        }
                    }
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
            System.exit(-1);
        }

        List<Map.Entry<String, Double>> list = map.entrySet().stream()
                .sorted(Map.Entry.comparingByValue(Comparator.reverseOrder()))
                .collect(Collectors.toList());

        try(PrintWriter writer = new PrintWriter(new FileWriter(ruleIndexFile, false))) {
            for (Map.Entry<String, Double> entry : list) {
                writer.println(entry.getKey() + "\n");
            }
        } catch (IOException e) {
            e.printStackTrace();
            System.exit(-1);
        }
    }

    public static void orderRules(File out) {
        File ruleFile = new File(out, "rules.txt");

        System.out.println("\n# GPFL System - Order Rules in File: " + ruleFile.getPath());

        Map<String, Double> map = new HashMap<>();
        try(LineIterator l = FileUtils.lineIterator(ruleFile)) {
            while(l.hasNext()) {
                String line = l.nextLine();
                if(!line.equals("")) {
                    map.put(line, Double.parseDouble(line.split("\t")[2]));
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
            System.exit(-1);
        }

        List<Map.Entry<String, Double>> list = map.entrySet().stream().sorted(Map.Entry.comparingByValue(Comparator.reverseOrder())).collect(Collectors.toList());
        try(PrintWriter writer = new PrintWriter(new FileWriter(ruleFile, false))) {
            for (Map.Entry<String, Double> entry : list) {
                writer.println(entry.getKey());
            }
        } catch (IOException e) {
            e.printStackTrace();
            System.exit(-1);
        }

        System.out.println("# Rules are ordered.");
    }

    public static void filterNonOverfittingRules(File config) {
        JSONObject object = Helpers.buildJSONObject(config);
        String home = object.getString("home");
        File out = new File(home, object.getString("out"));
        double overfittingFactor = object.getDouble("overfitting_factor");
        File ruleFile = new File(out, "rules.txt");
        File outFile = new File(out, "refined.txt");

        System.out.println("\n# GPFL System - Extracting Non-overfitting Rules from File: " + ruleFile.getPath());
        System.out.println("# overfitting factor = " + overfittingFactor);

        Map<String, Double> map = new HashMap<>();
        try(LineIterator l = FileUtils.lineIterator(ruleFile)) {
            while(l.hasNext()) {
                String line = l.nextLine();
                String[] values = line.split("\t");
                if(!line.equals("")) {
                    if(Double.parseDouble(values[4]) >= overfittingFactor)
                        map.put(line, Double.parseDouble(line.split("\t")[2]));
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
            System.exit(-1);
        }

        List<Map.Entry<String, Double>> list = map.entrySet().stream().sorted(Map.Entry.comparingByValue(Comparator.reverseOrder())).collect(Collectors.toList());
        try(PrintWriter writer = new PrintWriter(new FileWriter(outFile, false))) {
            for (Map.Entry<String, Double> entry : list) {
                writer.println(entry.getKey());
            }
        } catch (IOException e) {
            e.printStackTrace();
            System.exit(-1);
        }

        System.out.println("# Refined rule are saved to: " + outFile);
        System.out.println("# Rules are refined and ordered.");
    }

    public static Set<Rule> readRules(String target, File ruleIndexHome, GraphDatabaseService graph) {
        Logger.println("# Start Analyzing Target: " + target);
        Set<Rule> rules = new HashSet<>();
        File ruleIndexFile = new File(ruleIndexHome, target.replaceAll("[:/<>]", "_") + ".txt");
        try(Transaction tx = graph.beginTx()) {
            try (LineIterator l = FileUtils.lineIterator(ruleIndexFile)) {
                while (l.hasNext()) {
                    String line = l.nextLine();
                    if (line.startsWith("ABS: ")) {
                        Template rule = new Template(line.split("ABS: ")[1]);
                        if (!rule.isClosed()) {
                            String insRuleLine = l.nextLine();
                            for (String s : insRuleLine.split("\t")) {
                                SimpleInsRule insRule = new SimpleInsRule(rule, s);
                                insRule.insRuleString(graph);
                                rule.insRules.add(insRule);
                            }
                        } else {
                            String[] words = line.split("ABS: ")[1].split("\t");
                            rule.stats.setStandardConf(Double.parseDouble(words[3]));
                            rule.stats.setSmoothedConf(Double.parseDouble(words[4]));
                            rule.stats.setPcaConf(Double.parseDouble(words[5]));
                            rule.stats.setApcaConf(Double.parseDouble(words[6]));
                            rule.stats.setHeadCoverage(Double.parseDouble(words[7]));
                            rule.stats.setValidPrecision(Double.parseDouble(words[8]));
                        }
                        rules.add(rule);
                    }
                }
            } catch (Exception e) {
                e.printStackTrace();
                System.exit(-1);
            }
            tx.success();
        }

//        Logger.println(Context.analyzeRuleComposition("# Read Templates", rules));
        return rules;
    }

    public static boolean isTargetFunctional(Set<Pair> trainPairs) {
        Multimap<Long, Long> subToObjs = MultimapBuilder.hashKeys().hashSetValues().build();
        for (Pair trainPair : trainPairs) {
            subToObjs.put(trainPair.subId, trainPair.objId);
        }
        int functionalCount = 0;
        for (Long sub : subToObjs.keySet()) {
            if(subToObjs.get(sub).size() == 1)
                functionalCount++;
        }
        return ((double) functionalCount / subToObjs.keySet().size()) >= 0.9d;
    }

    public static Set<Pair> readExamples(File f, BiMap<String, Long> nodeIndex) {
        Set<Pair> pairs = new HashSet<>();
        try(LineIterator l = FileUtils.lineIterator(f)) {
            while(l.hasNext()) {
                String[] words = l.next().split("\t");
                if(words[1].equals(Settings.TARGET))
                    pairs.add(new Pair(words, nodeIndex));
            }
        } catch (IOException e) {
            e.printStackTrace();
            System.exit(-1);
        }
        return pairs;
    }

    public static Comparator<Rule> treeSetRuleQualityComparator(String metric) {
        return (o1, o2) -> {
            if(o1.getQuality(metric) - o2.getQuality(metric) >= 0) return -1;
            return 1;
        };
    }

    public static String parseRuleType(Rule rule) {
        if(rule instanceof Template)
            return rule.isClosed() ? "CAR" : "OAR";
        else {
            return rule.getType() == 0 ? "HAR" : "BAR";
        }
    }

    public static Rule parseRuleType(String type) {
        Rule rule = null;
        switch (type) {
            case "CAR":
                rule = new Template();
                rule.closed = true;
                break;
            case "OAR":
                rule = new Template();
                rule.closed = false;
                break;
            case "HAR":
                rule = new InstantiatedRule();
                rule.closed = false;
                rule.type = 0;
                break;
            case "BAR":
                rule = new InstantiatedRule();
                rule.closed = false;
                rule.type = 2;
                break;
        }
        assert rule != null;
        return rule;
    }

    public static String parseAtom(Atom atom) {
        List<String> words = new ArrayList<>();
        words.add(atom.direction.equals(Direction.OUTGOING) ? "+" : "-");
        words.add(atom.predicate);
        words.add(atom.subject);
        words.add(atom.object);
        return String.join(",", words);
    }

    public static Atom parseAtom(String atomString, BiMap<String, Long> nodeName2Id) {
        String[] words = atomString.split(",");
        assert words.length == 4;
        Atom atom = new Atom();
        atom.direction = words[0].equals("+") ? Direction.OUTGOING : Direction.INCOMING;
        atom.predicate = words[1];
        atom.type = RelationshipType.withName(words[1]);
        Predicate<String> isConstant = (s) ->
                !((s.startsWith("V") && s.length() == 2) || (s.equals("X") || s.equals("Y")));

        atom.subject = words[2];
        atom.subjectId = isConstant.test(atom.subject) ? nodeName2Id.get(atom.subject) : -1;

        atom.object = words[3];
        atom.objectId = isConstant.test(atom.object) ? nodeName2Id.get(atom.object) : -1;
        return atom;
    }

    public static void writeRules(File ruleFile, Collection<Rule> rules) {
        DecimalFormat f = new DecimalFormat("###.####");
        try(PrintWriter writer = new PrintWriter(new FileWriter(ruleFile, true))) {
            for (Rule rule : rules) {
                List<String> words = new ArrayList<>();
                words.add(parseRuleType(rule));
                words.add(parseAtom(rule.head));
                rule.bodyAtoms.forEach(atom -> words.add(parseAtom(atom)));
                words.add(f.format(rule.getSmoothedConf()));
                writer.println(String.join("\t", words));
            }
        } catch (IOException e) {
            e.printStackTrace();
            System.exit(-1);
        }
    }

    public static List<Rule> readRules(File ruleFile, BiMap<String, Long> nodeName2Id, String target) {
        List<Rule> rules = new ArrayList<>();
        try(LineIterator l = FileUtils.lineIterator(ruleFile)) {
            while(l.hasNext()) {
                String[] words = l.next().split("\t");
                Rule rule = parseRuleType(words[0]);
                rule.head = parseAtom(words[1], nodeName2Id);
                if(rule.head.predicate.equals(target)) {
                    for (int i = 2; i < words.length - 1; i++)
                        rule.bodyAtoms.add(parseAtom(words[i], nodeName2Id));
                    rule.stats.smoothedConf = Double.parseDouble(words[words.length - 1]);
                    rule.fromSubject = rule.head.subject.equals("X");
                    rules.add(rule);
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
            System.exit(-1);
        }
        return rules;
    }

    public static Set<String> readTargetsFromRules(File ruleFile, BiMap<String, Long> nodeIndex) {
        Set<String> targets = new HashSet<>();
        try(LineIterator l = FileUtils.lineIterator(ruleFile)) {
            while (l.hasNext()) {
                targets.add(parseAtom(l.next().split("\t")[1], nodeIndex).predicate);
            }
        } catch (IOException e) {
            e.printStackTrace();
            System.exit(-1);
        }
        return targets;
    }

    public static List<Rule> readRawRules(File ruleFile, BiMap<String, Long> nodeName2Id) {
        List<Rule> rules = new ArrayList<>();
        try(LineIterator l = FileUtils.lineIterator(ruleFile)) {
            while(l.hasNext()) {
                String[] words = l.next().split("\t");
                Rule rule = parseRuleType(words[1]);
                rule.index = Integer.parseInt(words[0]);
                rule.head = parseAtom(words[2], nodeName2Id);
                for (int i = 3; i < words.length; i++)
                    rule.bodyAtoms.add(parseAtom(words[i], nodeName2Id));
                rule.fromSubject = rule.head.subject.equals("X");
                rules.add(rule);
            }
        } catch (IOException e) {
            e.printStackTrace();
            System.exit(-1);
        }
        return rules;
    }

}
