package ac.uk.ncl.core;

import ac.uk.ncl.Settings;
import ac.uk.ncl.analysis.AnalysisUtils;
import ac.uk.ncl.structure.*;
import ac.uk.ncl.utils.*;
import ac.uk.ncl.validations.ValidRuleQuality;
import com.google.common.collect.Multimap;
import com.google.common.collect.MultimapBuilder;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.LineIterator;
import org.json.JSONArray;
import org.json.JSONObject;
import org.neo4j.graphdb.*;
import org.neo4j.graphdb.traversal.Traverser;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.text.DecimalFormat;
import java.text.MessageFormat;
import java.text.NumberFormat;
import java.util.*;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

public abstract class Engine {
    protected DecimalFormat format = new DecimalFormat("###.####");
    protected Runtime runtime = Runtime.getRuntime();

    protected GraphDatabaseService graph;
    protected GraphDatabaseService ruleGraph;

    protected JSONObject args;
    protected File home;
    protected File out;
    protected File trainFile;
    protected File testFile;
    protected File validFile;

    protected File ruleFile;
    protected File predictionFile;
    protected File verificationFile;
    protected File ruleIndexHome;
    protected File graphFile;

    protected Set<String> targets = new HashSet<>();
    protected int globalTargetCounter = 1;

    protected Engine(File config, String logName) {
        args = Helpers.buildJSONObject( config );
        home = new File(args.getString( "home" ));
        out = new File(home, args.getString("out"));
        out.mkdir();

        Logger.init(new File(out, logName + ".txt"), false);
        Logger.println("# Graph Path Feature Learning (GPFL) System\n" +
                "# Version: " + Settings.VERSION +  " | Date: " + Settings.DATE, 1);
        Logger.println(MessageFormat.format("# Cores: {0} | JVM RAM: {1}GB | Physical RAM: {2}GB"
                , runtime.availableProcessors()
                , Helpers.JVMRam()
                , Helpers.systemRAM()), 1);

        Settings.CONFIDENCE_OFFSET = Helpers.readSetting(args, "conf_offset", Settings.CONFIDENCE_OFFSET);
        Settings.TOP_K = Helpers.readSetting(args, "top_k", Settings.TOP_K);
        Settings.THREAD_NUMBER = Helpers.readSetting(args, "thread_number", Settings.THREAD_NUMBER);
        Settings.VERBOSITY = Helpers.readSetting(args, "verbosity", Settings.VERBOSITY);
        Settings.MIN_INSTANCES = Helpers.readSetting(args, "min_instances", Settings.MIN_INSTANCES);
        Settings.MAX_INSTANCES = Helpers.readSetting(args, "max_instances", Settings.MAX_INSTANCES);
        Settings.SATURATION = Helpers.readSetting(args, "saturation", Settings.SATURATION);
        Settings.BATCH_SIZE = Helpers.readSetting(args, "batch_size", Settings.BATCH_SIZE);
        Settings.NEO4J_IDENTIFIER = Helpers.readSetting(args, "neo4j_identifier", Settings.NEO4J_IDENTIFIER);
        Settings.VERIFY_RULE_SIZE = Helpers.readSetting(args, "verify_rule_size", Settings.VERIFY_RULE_SIZE);
        Settings.VERIFY_PREDICTION_SIZE = Helpers.readSetting(args, "verify_prediction_size", Settings.VERIFY_PREDICTION_SIZE);
        Settings.PRIOR_FILTERING = Helpers.readSetting(args, "prior_filtering", Settings.PRIOR_FILTERING);
        Settings.RANDOMLY_SELECTED_RELATIONS = Helpers.readSetting(args, "randomly_selected_relations", Settings.RANDOMLY_SELECTED_RELATIONS);
        Settings.RANDOM_WALKERS = Helpers.readSetting(args, "random_walkers", Settings.RANDOM_WALKERS);
        Settings.SUPPORT = Helpers.readSetting(args, "support", Settings.SUPPORT);
        Settings.CONF = Helpers.readSetting(args, "standard_conf", Settings.CONF);
        Settings.HEAD_COVERAGE = Helpers.readSetting(args, "head_coverage", Settings.HEAD_COVERAGE);
        Settings.QUALITY_MEASURE = Helpers.readSetting(args, "quality_measure", Settings.QUALITY_MEASURE);
        Settings.VALID_PRECISION = Helpers.readSetting(args, "valid_precision", Settings.VALID_PRECISION);
        Settings.HIGH_QUALITY = Helpers.readSetting(args, "high_quality", Settings.HIGH_QUALITY);
        Settings.USE_SIGMOID = Helpers.readSetting(args, "use_sigmoid", Settings.USE_SIGMOID);
        Settings.OVERFITTING_FACTOR = Helpers.readSetting(args, "overfitting_factor", Settings.OVERFITTING_FACTOR);

        Settings.INS_DEPTH = Helpers.readSetting(args, "ins_depth", Settings.INS_DEPTH);
        Settings.CAR_DEPTH = Helpers.readSetting(args, "car_depth", Settings.CAR_DEPTH);
        Settings.DEPTH = Math.max(Settings.INS_DEPTH, Settings.CAR_DEPTH);

        Settings.SPEC_TIME = Helpers.readSettingConditionMax(args, "spec_time", Settings.SPEC_TIME);
        Settings.GEN_TIME = Helpers.readSettingConditionMax(args, "gen_time", Settings.GEN_TIME);
        Settings.ESSENTIAL_TIME = Helpers.readSettingConditionMax(args, "essential_time", Settings.ESSENTIAL_TIME);

        Settings.LEARN_GROUNDINGS = Helpers.readSettingConditionMax(args, "learn_groundings", Settings.LEARN_GROUNDINGS);
        Settings.APPLY_GROUNDINGS = Helpers.readSettingConditionMax(args, "apply_groundings", Settings.APPLY_GROUNDINGS);
        Settings.SUGGESTION_CAP = Helpers.readSettingConditionMax(args, "suggestion_cap", Settings.SUGGESTION_CAP);
        Settings.INS_RULE_CAP = Helpers.readSettingConditionMax(args, "ins_rule_cap", Settings.INS_RULE_CAP);

        Settings.RULE_GRAPH = Helpers.readSetting(args, "rule_graph", Settings.RULE_GRAPH);
        if(Settings.RULE_GRAPH) {
            Logger.println("# Initialize Rule Graph at: " + (new File(out, "RuleGraph/databases/graph.db")).getPath(), 1);
            ruleGraph = GraphOps.createEmptyGraph(new File( out, "RuleGraph"));
        }
    }

    public static GraphDatabaseService buildGraph(String home) {
        GraphDatabaseService graph;
        File graphFile = new File(home, "databases/graph.db");
        File trainFile = new File(home, "data/train.txt");
        File validFile = new File(home, "data/valid.txt");
        File testFile = new File(home, "data/test.txt");
        graph = GraphBuilder.populateGraphFromTriples(graphFile, trainFile, validFile, testFile);
        Runtime.getRuntime().addShutdownHook(new Thread(graph::shutdown));
        return graph;
    }

    public static GraphDatabaseService buildGraphSingleFile(String home) {
        File graphFile = new File(home, "databases/graph.db");
        File trainFile = new File(home, "data/train.txt");
        GraphDatabaseService graph = GraphBuilder.populateGraphFromTriples(graphFile, trainFile);
        Runtime.getRuntime().addShutdownHook(new Thread(graph::shutdown));
        return graph;
    }

    public static void selectTargets(File config) {
        JSONObject args = Helpers.buildJSONObject(config);
        String home = args.getString("home");
        Settings.MIN_INSTANCES = Helpers.readSetting(args, "min_instances", Settings.MIN_INSTANCES);
        Settings.MAX_INSTANCES = Helpers.readSetting(args, "max_instances", Settings.MAX_INSTANCES);
        Settings.RANDOMLY_SELECTED_RELATIONS = Helpers.readSetting(args, "randomly_selected_relations", Settings.RANDOMLY_SELECTED_RELATIONS);

        System.out.println(MessageFormat.format("# Min Instances: {0} | Max Instances: {1} | " +
                "Randomly Selected Targets: {2}", Settings.MIN_INSTANCES, Settings.MAX_INSTANCES, Settings.RANDOMLY_SELECTED_RELATIONS));

        CountedSet<String> counter = new CountedSet<>();
        try(LineIterator l = FileUtils.lineIterator(new File(home, "data/train.txt"))) {
            while(l.hasNext()) {
                String target = l.nextLine().split("\t")[1];
                counter.add(target);
            }
        } catch (IOException e) {
            e.printStackTrace();
            System.exit(-1);
        }

        System.out.println("# All Targets: " + counter.size());

        List<String> selected = new ArrayList<>();
        for (String target : counter) {
            if(counter.get(target) <= Settings.MAX_INSTANCES && counter.get(target) >= Settings.MIN_INSTANCES)
                selected.add(target);
        }

        Collections.shuffle(selected);
        selected = selected.subList(0, Math.min(Settings.RANDOMLY_SELECTED_RELATIONS, selected.size()))
                .stream().map(target -> '"' + target + '"').collect(Collectors.toList());
        if(!selected.isEmpty())
            System.out.println("# " + String.join(",", selected));
        else
            System.out.println("# Cannot find any target met the constraints.");
    }

    public static void verifyGraph(String home) {
        File trainFile = new File(home, "data/train.txt");
        File validFile = new File(home, "data/valid.txt");
        File testFile = new File(home, "data/test.txt");
        File graphFile = new File(home, "databases/graph.db");

        GraphDatabaseService graph = GraphBuilder.populateGraphFromTriples(graphFile, trainFile, validFile, testFile);

        Set<Triple> trainTriples = Evaluator.readTriples(trainFile);
        Set<Triple> validTriples = Evaluator.readTriples(validFile);
        Set<Triple> testTriples = Evaluator.readTriples(testFile);

        Set<String> entitiesFromTriple = new HashSet<>();
        Set<String> typesFromTriple = new HashSet<>();
        trainTriples.forEach( triple -> {
            entitiesFromTriple.add(triple.obj);
            entitiesFromTriple.add(triple.sub);
            typesFromTriple.add(triple.pred);
        });
        validTriples.forEach( triple -> {
            entitiesFromTriple.add(triple.obj);
            entitiesFromTriple.add(triple.sub);
            typesFromTriple.add(triple.pred);
        });
        testTriples.forEach( triple -> {
            entitiesFromTriple.add(triple.obj);
            entitiesFromTriple.add(triple.sub);
            typesFromTriple.add(triple.pred);
        });

        System.out.println("All entities from file: " + entitiesFromTriple.size());
        System.out.println("All types from file: " + typesFromTriple.size());

        Set<String> entitiesFromGraph = new HashSet<>();
        Set<String> typesFromGraph = new HashSet<>();

        try(Transaction tx = graph.beginTx()) {
            graph.getAllNodes().forEach( node -> {
                entitiesFromGraph.add((String) node.getProperty("name"));
            });
            graph.getAllRelationshipTypes().forEach( type -> {
                typesFromGraph.add(type.name());
            });
            tx.success();
        }

        System.out.println("All entities from graph: " + entitiesFromGraph.size());
        System.out.println("All types from graph: " + typesFromGraph.size());

        int entitySize = entitiesFromTriple.size();
        int typeSize = typesFromTriple.size();
        entitiesFromTriple.addAll(entitiesFromGraph);
        typesFromTriple.addAll(typesFromGraph);

        assert  entitiesFromTriple.size() == entitySize;
        assert  typesFromTriple.size() == typeSize;

        System.out.println("Test Passed.");
    }

    public static GraphDatabaseService createRandomSplitsFromGraph(File config) {
        System.out.println("# GPFL - Split Triples into Training/Test/Valid sets: ");

        boolean allTargets = false;
        JSONObject args = Helpers.buildJSONObject(config);
        String home = args.getString("home");
        Settings.NEO4J_IDENTIFIER = Helpers.readSetting(args, "neo4j_identifier", Settings.NEO4J_IDENTIFIER);
        double[] splitRatio = IO.readSplitRatio(args.getJSONArray("split_ratio"));
        JSONArray array = args.getJSONArray("target_relation");
        Set<String> targets = new HashSet<>();
        for (Object o : array) {
            targets.add((String) o);
        }

        System.out.println(MessageFormat.format("# Split Ratio (Train/Test/Valid): {0}/{1}/{2}"
                , splitRatio[0]
                , splitRatio[1]
                , Math.abs(1d - splitRatio[0] - splitRatio[1])));

        if(targets.isEmpty()) {
            System.out.println("\n# Create Splits for all predicates.");
            allTargets = true;
        }
        else
            System.out.println("# Create Splits for Selected Targets:\n# "
                    + String.join(", ", targets));

        Multimap<String, Pair> pairMap = MultimapBuilder.hashKeys().hashSetValues().build();
        GraphDatabaseService graph = IO.loadGraph(new File(home, "databases/graph.db"));
        Runtime.getRuntime().addShutdownHook(new Thread(graph::shutdown));

        File dataFolder = new File(home, "data");
        dataFolder.mkdir();

        int trainTriples = 0, testTriples = 0, validTriples = 0, beforeRemovalGraphSize = 0;
        long afterRemovalGraphSize = 0;
        try(Transaction tx = graph.beginTx()) {
            PrintWriter[] writers = new PrintWriter[] {
                    new PrintWriter(new File(dataFolder, "train.txt")),
                    new PrintWriter(new File(dataFolder, "test.txt")),
                    new PrintWriter(new File(dataFolder, "valid.txt")),
                    new PrintWriter(new File(dataFolder, "annotated_train.txt")),
                    new PrintWriter(new File(dataFolder, "annotated_test.txt")),
                    new PrintWriter(new File(dataFolder, "annotated_valid.txt"))
            };
            for (Relationship rel : graph.getAllRelationships()) {
                beforeRemovalGraphSize++;
                Node start = rel.getStartNode();
                Node end = rel.getEndNode();
                String relName = rel.getType().name();
                Pair pair = new Pair(start.getId(), (String) start.getProperty(Settings.NEO4J_IDENTIFIER),
                        end.getId(), (String) end.getProperty(Settings.NEO4J_IDENTIFIER),
                        rel.getId(), relName);
                if(targets.contains(relName) || allTargets) {
                    pairMap.put(relName, pair);
                } else {
                    writers[0].println(pair.toTripleString());
                    writers[3].println(pair.toAnnotatedString());
                    trainTriples++;
                }
            }

            for (String target : pairMap.keySet()) {
                List<Pair> pairs = new ArrayList<>(pairMap.get(target));
                int trainBoundary = (int) Math.floor(pairs.size() * splitRatio[0]);
                int testBoundary = (int) Math.ceil(pairs.size() * splitRatio[1]);
                for (Pair pair : pairs.subList(0, trainBoundary)) {
                    writers[0].println(pair.toTripleString());
                    writers[3].println(pair.toAnnotatedString());
                    trainTriples++;
                }
                for (Pair pair : pairs.subList(trainBoundary, trainBoundary + testBoundary)) {
                    graph.getRelationshipById(pair.relId).delete();
                    pair.relId = -1;
                    writers[1].println(pair.toTripleString());
                    writers[4].println(pair.toAnnotatedString());
                    testTriples++;
                }
                for (Pair pair : pairs.subList(trainBoundary + testBoundary, pairs.size())) {
                    graph.getRelationshipById(pair.relId).delete();
                    pair.relId = -1;
                    writers[2].println(pair.toTripleString());
                    writers[5].println(pair.toAnnotatedString());
                    validTriples++;
                }
            }

            afterRemovalGraphSize = graph.getAllRelationships().stream().count();

            for (PrintWriter writer : writers) {
                writer.close();
            }
            tx.success();
        } catch (IOException e) {
            e.printStackTrace();
            System.exit(-1);
        }

        System.out.println(MessageFormat.format("# Training: {0} | Test: {1} | Valid: {2}"
                , trainTriples, testTriples, validTriples));
        assert testTriples + validTriples + afterRemovalGraphSize == beforeRemovalGraphSize;
        System.out.println("# Original Graph Size: " + beforeRemovalGraphSize + " | With Test/Valid Removed: " + afterRemovalGraphSize);
        return graph;
    }

    /**
     * Setup:
     * need database, test and valid ready.
     * This procedure will create annotated train, test and valid files based on the
     * test and valid files produced on a similar knowledge graph such that we can
     * ensure both knowledge graphs share the same test and valid triples. This is
     * used to evaluate if newly added triples contribute to performance improvement.
     * @param config
     * @return
     */
    public static GraphDatabaseService createGuidedRandomSplitsFromGraph(File config) {
        JSONObject args = Helpers.buildJSONObject(config);
        String home = args.getString("home");
        Settings.NEO4J_IDENTIFIER = Helpers.readSetting(args, "neo4j_identifier", Settings.NEO4J_IDENTIFIER);
        File dataFolder = new File(home, "data");
        dataFolder.mkdir();

        List<Pair> testPairs = new ArrayList<>();
        List<Pair> validPairs = new ArrayList<>();
        GraphDatabaseService graph = IO.loadGraph(new File(home, "databases/graph.db"));
        Runtime.getRuntime().addShutdownHook(new Thread(graph::shutdown));

        try {
            LineIterator anTestL = FileUtils.lineIterator(new File(dataFolder, "test.txt"));
            LineIterator anValidL = FileUtils.lineIterator(new File(dataFolder, "valid.txt"));
            while(anTestL.hasNext()) {
                String[] words = anTestL.nextLine().split("\t");
                testPairs.add(new Pair(words[0], words[1], words[2]));
            }
            while(anValidL.hasNext()) {
                String[] words = anValidL.nextLine().split("\t");
                validPairs.add(new Pair(words[0], words[1], words[2]));
            }
            anTestL.close();
            anValidL.close();
        } catch (IOException e) {
            e.printStackTrace();
            System.exit(-1);
        }


        int trainTriples = 0, testTriples = 0, validTriples = 0, beforeRemovalGraphSize = 0;
        long afterRemovalGraphSize = 0;
        boolean in = false;
        try(Transaction tx = graph.beginTx()) {
            PrintWriter[] writers = new PrintWriter[] {
                    new PrintWriter(new File(dataFolder, "train.txt")),
                    new PrintWriter(new File(dataFolder, "annotated_train.txt")),
                    new PrintWriter(new File(dataFolder, "annotated_test.txt")),
                    new PrintWriter(new File(dataFolder, "annotated_valid.txt"))
            };
            for (Relationship rel : graph.getAllRelationships()) {
                beforeRemovalGraphSize++;
                Node start = rel.getStartNode();
                Node end = rel.getEndNode();
                String relName = rel.getType().name();
                Pair pair = new Pair(start.getId(), (String) start.getProperty(Settings.NEO4J_IDENTIFIER),
                        end.getId(), (String) end.getProperty(Settings.NEO4J_IDENTIFIER),
                        rel.getId(), relName);

                for (Pair testPair : testPairs) {
                    if(testPair.insEqual(pair)) {
                        testTriples++;
                        in = true;
                        graph.getRelationshipById(pair.relId).delete();
                        pair.relId = -1;
                        writers[2].println(pair.toAnnotatedString());
                        break;
                    }
                }

                if(!in) {
                    for (Pair validPair : validPairs) {
                        if(validPair.insEqual(pair)) {
                            validTriples++;
                            in = true;
                            graph.getRelationshipById(pair.relId).delete();
                            pair.relId = -1;
                            writers[3].println(pair.toAnnotatedString());
                            break;
                        }
                    }
                }

                if(!in) {
                    writers[0].println(pair.toTripleString());
                    writers[1].println(pair.toAnnotatedString());
                    trainTriples++;
                }
                in = false;
            }

            System.out.println("# Before Removal Graph Size: " + beforeRemovalGraphSize);
            System.out.println(MessageFormat.format("# Train: {0} | Test: {1} | Valid: {2}" +
                            "\n# Test Diff: {3} | Valid Diff: {4}"
                    , trainTriples, testTriples, validTriples
                    , testPairs.size() - testTriples
                    , validPairs.size() - validTriples));

            afterRemovalGraphSize = graph.getAllRelationships().stream().count();
            assert testTriples + validTriples + afterRemovalGraphSize == beforeRemovalGraphSize;
            System.out.println("# After Removal Graph size: " + afterRemovalGraphSize);

            for (PrintWriter writer : writers) {
                writer.close();
            }
            tx.success();
        } catch (IOException e) {
            e.printStackTrace();
            System.exit(-1);
        }

        return graph;
    }

    public static void createTripleFileFromGraph(File config) {
        JSONObject args = Helpers.buildJSONObject(config);
        String home = args.getString("home");
        Settings.NEO4J_IDENTIFIER = Helpers.readSetting(args, "neo4j_identifier", Settings.NEO4J_IDENTIFIER);
        GraphDatabaseService graph = IO.loadGraph(new File(home, "databases/graph.db"));

        File dataHome = new File(home, "data");
        dataHome.mkdir();
        File trainFile = new File(dataHome, "train.txt");

        try(Transaction tx = graph.beginTx()) {
            try(PrintWriter writer = new PrintWriter(new FileWriter(trainFile, false))) {
                int count = 0;
                for (Relationship relationship : graph.getAllRelationships()) {
                    String sub = (String) relationship.getStartNode().getProperty(Settings.NEO4J_IDENTIFIER);
                    String obj = (String) relationship.getEndNode().getProperty(Settings.NEO4J_IDENTIFIER);
                    String rel = relationship.getType().name();
                    writer.println(String.join("\t", new String[]{sub, rel, obj}));
                    if(++count % 200000 == 0) {
                        System.out.println("# Write " + count + " Triples.");
                    }
                }
            } catch (IOException e) {
                e.printStackTrace();
                System.exit(-1);
            }
            tx.success();
        }
    }

    public static void createRandomSplitsFromFiles(File config) {
        System.out.println("# GPFL - Split Triples into Train/Test/Valid sets: ");

        JSONObject args = Helpers.buildJSONObject(config);
        double[] splitRatio = IO.readSplitRatio(args.getJSONArray("split_ratio"));

        System.out.println(MessageFormat.format("# Split Ratio (Train/Test/Valid): {0}/{1}/{2}"
                , splitRatio[0]
                , splitRatio[1]
                , 1d - splitRatio[0] - splitRatio[1]));

        String home = args.getString("home");
        Multimap<String, Triple> tripleMap = MultimapBuilder.hashKeys().hashSetValues().build();
        File[] files = new File[]{new File(home, "data/train.txt")
                , new File(home, "data/test.txt"), new File(home, "data/valid.txt")};

        for (File file : files) {
            tripleMap.putAll(IO.readTripleMap(file));
        }

        System.out.println("# All Triples: " + tripleMap.size());
        splitAndWrite(files, tripleMap, splitRatio);
    }

    private static void splitAndWrite(File[] files, Multimap<String, Triple> tripleMap, double[] splitRatio) {
        Multimap<File, Triple> splitResults = MultimapBuilder.hashKeys().hashSetValues().build();
        for (String type : tripleMap.keySet()) {
            List<Triple> triples = new ArrayList<>(tripleMap.get(type));
            int trainBoundary = (int) Math.floor(triples.size() * splitRatio[0]);
            int testBoundary = (int) Math.ceil(triples.size() * splitRatio[1]);
            splitResults.putAll(files[0], triples.subList(0, trainBoundary));
            splitResults.putAll(files[1], triples.subList(trainBoundary, trainBoundary + testBoundary));
            splitResults.putAll(files[2], triples.subList(trainBoundary + testBoundary, triples.size()));
        }

        System.out.println(MessageFormat.format("# Triple Sizes (Train/Test/Valid): {0}/{1}/{2}"
                , splitResults.get(files[0]).size()
                , splitResults.get(files[1]).size()
                , splitResults.get(files[2]).size()));

        for (File file : splitResults.keySet()) {
            IO.writeTriples(file, new HashSet<>(splitResults.get(file)));
        }
    }

    /**
     * Create Test and Valid sets from existing ones by sampling a set of targets. The sampled targets
     * can be manually picked by setting `target_relation` option in `config.json` file, or randomly
     * selected by setting `randomly_selected_relations`.
     *
     * If the random selection mode is on, the randomly selected targets must:
     * 1.) Appear in both test and valid files
     * 2.) Have at least `minInstance` instances
     *
     * When the manual selection is on, the above mentioned constraints are not forced.
     *
     * Manual selection has higher priority than manual selection if both are set to an non-default value.
     *
     * @param config Configuration File
     * @param minInstance Minimal number of instances a qualified target must have in
     *                    both test and valid file.
     */
    public static void sampleTargets(File config, int minInstance) {
        JSONObject args = Helpers.buildJSONObject(config);
        String home = args.getString("home");

        File validFile = new File(home, "data/valid.txt");
        File testFile = new File(home, "data/test.txt");

        try {
            Multimap<String, Triple> testTripleMap = IO.readTripleMap(testFile);
            AnalysisUtils.analyzeTripleMap("Test", testTripleMap);
            System.out.println();

            Multimap<String, Triple> validTripleMap = IO.readTripleMap(validFile);
            AnalysisUtils.analyzeTripleMap("Valid", validTripleMap);
            System.out.println();

            Set<String> joinedTargets = Helpers.joinSets(testTripleMap.keySet(), validTripleMap.keySet());
            Set<String> qualifiedTestTargets = selectTargets(args, joinedTargets, testTripleMap, minInstance);
            System.out.println("# Qualified Test Targets: " + qualifiedTestTargets.size());

            Set<String> qualifiedValidTargets = selectTargets(args, joinedTargets, validTripleMap, minInstance);
            System.out.println("# Qualified Valid Targets: " + qualifiedValidTargets.size());

            if(args.getInt("randomly_selected_relations") != 0 && args.getJSONArray("target_relation").isEmpty()) {
                List<String> joinedTargetList = new ArrayList<>(Helpers.joinSets(qualifiedTestTargets, qualifiedValidTargets));
                Collections.shuffle(joinedTargetList);
                joinedTargets = new HashSet<>(joinedTargetList
                        .subList(0, Math.min(joinedTargetList.size(), args.getInt("randomly_selected_relations"))));
            } else {
                joinedTargets = Helpers.joinSets(qualifiedTestTargets, qualifiedValidTargets);
            }

            System.out.println("# Selected Targets: ");
            for (String joinedTarget : joinedTargets) {
                System.out.println("# " + joinedTarget + "\n# Instances in Test: " + testTripleMap.get(joinedTarget).size()
                        + " | Instances in Valid: " + validTripleMap.get(joinedTarget).size());
            }

            IO.writeTriples(testFile, joinedTargets, testTripleMap);
            IO.writeTriples(validFile, joinedTargets, validTripleMap);
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(-1);
        }
    }

    private static Set<String> selectTargets(JSONObject args, final Set<String> targets
            , final Multimap<String, Triple> tripleMap
            , int minInstance) throws Exception {
        Set<String> results = new HashSet<>();
        JSONArray array = args.getJSONArray("target_relation");
        if(!array.isEmpty()) {
            for (Object target : array) {
                if(!targets.contains((String) target)) {
                    throw new Exception("Selected targets not found in the file.");
                } else
                    results.add((String) target);
            }
        } else {
            for (String target : targets) {
                if(tripleMap.get(target).size() >= minInstance)
                    results.add(target);
            }
        }
        return results;
    }

    public void populateTargets() {
        if(testFile != null)
            targets = IO.readTargets(testFile);
        else {
            try(Transaction tx = graph.beginTx()) {
                for (RelationshipType type : graph.getAllRelationshipTypes())
                    targets.add(type.name());
                tx.success();
            }
        }

        Set<String> selectedTargets = new HashSet<>();
        JSONArray array = args.getJSONArray("target_relation");
        if(!array.isEmpty()) {
            for (Object o : array) {
                String target = (String) o;
                if(targets.contains(target))
                    selectedTargets.add(target);
                else {
                    System.err.println("# Selected Targets do not exist in the test file.");
                    System.exit(-1);
                }
            }
            targets = selectedTargets;
        } else {
            int randomSelect = args.getInt("randomly_selected_relations");
            if(randomSelect != 0) {
                List<String> targetList = new ArrayList<>(targets);
                Collections.shuffle(targetList);
                targets = new HashSet<>(targetList.subList(0, Math.min(randomSelect, targetList.size())));
            }
        }
    }

    public void generalizationSequential(Set<Pair> trainPairs, Context context) {
        long s = System.currentTimeMillis();
        List<Pair> trainPairList = new ArrayList<>(trainPairs);
        Set<Pair> visitedTrainPairs = new HashSet<>();
        Set<Rule> previousBatch = new HashSet<>();
        Set<Rule> currentBatch = new HashSet<>();
        double saturation = 0d;
        int pathCount = 0;
        Random rand = new Random();

        try(Transaction tx = graph.beginTx()) {
            do {
                Pair pair = trainPairList.get(rand.nextInt(trainPairs.size()));
                visitedTrainPairs.add(pair);
                Traverser traverser = GraphOps.buildStandardTraverser(graph, pair, Settings.RANDOM_WALKERS);
                for (Path path : traverser) {
                    if (++pathCount % Settings.BATCH_SIZE == 0) {
                        int overlap = 0;
                        for (Rule rule : currentBatch) {
                            if(previousBatch.contains(rule))
                                overlap++;
                        }
                        saturation = (double) overlap / currentBatch.size();
                        previousBatch.addAll(currentBatch);
                        currentBatch = new HashSet<>();
                    }
                    Rule abstractRule = context.abstraction(path, pair);
                    currentBatch.add(abstractRule);
                }
            } while (saturation < Settings.SATURATION);
            tx.success();
        }

        Logger.println(MessageFormat.format("# Visited/All Train Pairs: {0}/{1} | Ratio: {2}%\n" +
                        "# Sampled Paths: {3}"
                , visitedTrainPairs.size()
                , trainPairs.size()
                , new DecimalFormat("###.##").format(((double) visitedTrainPairs.size() / trainPairs.size()) * 100f)
                , pathCount));
        GlobalTimer.updateTemplateGenStats(Helpers.timerAndMemory(s, "# Generalization"));
        Logger.println(Context.analyzeRuleComposition("# Generated Templates"
                , context.getAbstractRules()), 1);
    }

    public void generalization(Set<Pair> trainPairs, Context context) {
        long s = System.currentTimeMillis();
        BlockingQueue<Rule> ruleQueue = new LinkedBlockingDeque<>(Settings.BATCH_SIZE * 2);
        Set<Pair> visitedTrainPairs = new HashSet<>();

        RuleProducer[] producers = new RuleProducer[Settings.THREAD_NUMBER];
        RuleConsumer consumer = new RuleConsumer(0, ruleQueue, context);
        for (int i = 0; i < producers.length; i++) {
            producers[i] = new RuleProducer(i, ruleQueue, trainPairs, visitedTrainPairs, graph, consumer);
        }
        try {
            for (RuleProducer producer : producers) {
                producer.join();
            }
            consumer.join();
        } catch (InterruptedException e) {
            e.printStackTrace();
            System.exit(-1);
        }

        Logger.println(MessageFormat.format("# Visited/Training Instances: {0}/{1} | Ratio: {2}%" +
                        " | Sampled Paths: {3} | Saturation: {4}%"
                , visitedTrainPairs.size()
                , trainPairs.size()
                , new DecimalFormat("###.##").format(((double) visitedTrainPairs.size() / trainPairs.size()) * 100f)
                , consumer.getPathCount()
                , new DecimalFormat("###.##").format(consumer.getSaturation() * 100f))
        );
        GlobalTimer.updateTemplateGenStats(Helpers.timerAndMemory(s, "# Generalization"));
        Logger.println(Context.analyzeRuleComposition("# Generated Templates"
                , context.getAbstractRules()), 1);
    }

    static class RuleProducer extends Thread {
        int id;
        List<Pair> trainPairs;
        BlockingQueue<Rule> ruleQueue;
        GraphDatabaseService graph;
        Thread consumer;
        Set<Pair> visitedTrainPairs;

        RuleProducer(int id, BlockingQueue<Rule> ruleQueue, Set<Pair> trainPairs, Set<Pair> visitedTrainPairs
                , GraphDatabaseService graph, Thread consumer) {
            super("RuleProducer-" + id);
            this.id = id;
            this.ruleQueue = ruleQueue;
            this.trainPairs = new ArrayList<>(trainPairs);
            this.graph = graph;
            this.consumer = consumer;
            this.visitedTrainPairs = visitedTrainPairs;
            start();
        }

        @Override
        public void run() {
            Random rand = new Random();
            try(Transaction tx = graph.beginTx()) {
                while(consumer.isAlive()) {
                    Pair pair = trainPairs.get(rand.nextInt(trainPairs.size()));
                    addVisitedPair(pair);
                    Traverser traverser = GraphOps.buildStandardTraverser(graph, pair, Settings.RANDOM_WALKERS);
                    for (Path path : traverser) {
                        Rule rule = Context.createTemplate(path, pair);
                        while (consumer.isAlive()) {
                            if (ruleQueue.offer(rule, 100, TimeUnit.MILLISECONDS))
                                break;
                        }
                        if (!consumer.isAlive())
                            break;
                    }
                }
                tx.success();
            } catch (InterruptedException e) {
                e.printStackTrace();
                System.exit(-1);
            }
        }

        private synchronized void addVisitedPair(Pair pair) {
            visitedTrainPairs.add(pair);
        }
    }

    static class RuleConsumer extends Thread {
        int id;
        BlockingQueue<Rule> ruleQueue;
        Context context;
        int pathCount = 0;
        double saturation = 0d;

        RuleConsumer(int id, BlockingQueue<Rule> ruleQueue, Context context) {
            super("RuleConsumer-" + id);
            this.id = id;
            this.ruleQueue = ruleQueue;
            this.context = context;
            GlobalTimer.setGenStartTime(System.currentTimeMillis());
            start();
        }

        @Override
        public void run() {
            Set<Rule> currentBatch = new HashSet<>();
            do {
                if(pathCount % Settings.BATCH_SIZE == 0) {
                    int overlaps = 0;
                    if(currentBatch.isEmpty() && pathCount != 0)
                        break;

                    for (Rule rule : currentBatch) {
                        if(context.getAbstractRules().contains(rule))
                            overlaps++;
                        else
                            context.updateFreqAndIndex(rule);
                    }

                    saturation = currentBatch.isEmpty() ? 0d : (double) overlaps / currentBatch.size();
                    currentBatch.clear();
                }
                Rule rule = ruleQueue.poll();
                if(rule != null) {
                    if(rule.isClosed() ? rule.length() <= Settings.CAR_DEPTH : rule.length() <= Settings.INS_DEPTH)
                        currentBatch.add(rule);
                    pathCount++;
                }
            } while (saturation < Settings.SATURATION && !GlobalTimer.stopGen());
            for (Rule rule : currentBatch) {
                context.updateFreqAndIndex(rule);
            }
        }

        public int getPathCount() {
            return pathCount;
        }

        public double getSaturation() {
            return saturation;
        }
    }

    public void specialization(Context context, Set<Pair> trainPairs, Set<Pair> validPairs, File ruleIndexFile) {
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
        BlockingQueue<String> tempFileContents = new LinkedBlockingDeque<>(1000000);
        BlockingQueue<String> ruleFileContents = new LinkedBlockingDeque<>(1000000);

        GlobalTimer.setSpecStartTime(System.currentTimeMillis());
        SpecializationTask[] tasks = new SpecializationTask[Settings.THREAD_NUMBER];
        for (int i = 0; i < tasks.length; i++) {
            tasks[i] = new SpecializationTask(i, graph, abstractRuleQueue
                    , trainPairs, validPairs, objOriginalMap, subOriginalMap, validObjToSub, validSubToObj
                    , context, tempFileContents, ruleFileContents);
        }
        RuleWriter tempFileWriter = new RuleWriter(0, tasks, ruleIndexFile, tempFileContents, true);
        RuleWriter ruleFileWriter = new RuleWriter(0, tasks, ruleFile, ruleFileContents, true);
        try {
            for (SpecializationTask task : tasks) {
                task.join();
            }
            tempFileWriter.join();
            ruleFileWriter.join();
        } catch (InterruptedException e) {
            e.printStackTrace();
            System.exit(-1);
        }

        GlobalTimer.updateInsRuleStats(Helpers.timerAndMemory(s,"# Specialization"));
        Logger.println(Context.analyzeRuleComposition("# Specialized Templates", context.getSpecializedRules()), 1);
        Logger.println("# All Instantiated Rules: " + f.format(context.getTotalInsRules() + context.getEssentialRules()), 1);
    }

    public void ruleApplication(Context context, File ruleIndexHome) {
        Logger.println("\n# Start Rule Application", 2);
        NumberFormat f = NumberFormat.getNumberInstance(Locale.US);
        long s = System.currentTimeMillis();
        context.initPredictionMap();

        BlockingQueue<Rule> abstractRuleQueue = new LinkedBlockingDeque<>(100000);
        RuleApplicationTask[] tasks = new RuleApplicationTask[Settings.THREAD_NUMBER];
        RuleReader reader = new RuleReader(0, ruleIndexHome, abstractRuleQueue, context);
        for (int i = 0; i < tasks.length; i++) {
            tasks[i] = new RuleApplicationTask(i, graph, abstractRuleQueue, context, reader);
        }
        try {
            for (RuleApplicationTask task : tasks) {
                task.join();
            }
            reader.join();
        } catch (InterruptedException e) {
            e.printStackTrace();
            System.exit(-1);
        }

        GlobalTimer.updateRuleApplyStats(Helpers.timerAndMemory(s,"# Rule Application"));
        Logger.println("# Predictions: " + f.format(context.predictionMapSize()), 2);
        Logger.println(Context.analyzeRuleComposition("# Applied Rules", context.getAppliedRules()), 2);
    }

    static class SpecializationTask extends Thread {
        int id;
        GraphDatabaseService graph;
        BlockingQueue<Rule> abstractRuleQueue;
        BlockingQueue<String> tempFileContents;
        Context context;
        Set<Pair> trainPairs;
        Set<Pair> validPairs;
        Multimap<Long, Long> objOriginalMap;
        Multimap<Long, Long> subOriginalMap;
        Multimap<Long, Long> validObjToSub;
        Multimap<Long, Long> validSubToObj;
        BlockingQueue<String> ruleFileContents;

        public SpecializationTask(int id
                , GraphDatabaseService graph
                , BlockingQueue<Rule> abstractRuleQueue
                , Set<Pair> trainPairs
                , Set<Pair> validPairs
                , Multimap<Long, Long> objOriginalMap
                , Multimap<Long, Long> subOriginalMap
                , Multimap<Long, Long> validObjToSub
                , Multimap<Long, Long> validSubToObj
                , Context context
                , BlockingQueue<String> tempFileContents
                , BlockingQueue<String> ruleFileContents) {
            super("InstantiationTask-" + id);
            this.id = id;
            this.graph = graph;
            this.abstractRuleQueue = abstractRuleQueue;
            this.tempFileContents = tempFileContents;
            this.trainPairs = trainPairs;
            this.objOriginalMap = objOriginalMap;
            this.subOriginalMap = subOriginalMap;
            this.context = context;
            this.ruleFileContents = ruleFileContents;
            this.validObjToSub = validObjToSub;
            this.validSubToObj = validSubToObj;
            this.validPairs = validPairs;
            start();
        }

        @Override
        public void run() {
            try(Transaction tx = graph.beginTx()) {
                while (!abstractRuleQueue.isEmpty() && !GlobalTimer.stopSpec() && context.getTotalInsRules() < Settings.INS_RULE_CAP) {
                    Template abstractRule = (Template) abstractRuleQueue.poll();
                    if(abstractRule != null) {
                        Multimap<Long, Long> anchoringToOriginalMap = abstractRule.isFromSubject() ? objOriginalMap : subOriginalMap;
                        Multimap<Long, Long> validOriginals = abstractRule.isFromSubject() ? validObjToSub : validSubToObj;
                        abstractRule.specialization(graph, trainPairs, validPairs
                                , anchoringToOriginalMap, validOriginals, context
                                , ruleFileContents, tempFileContents);
                    }
                }
                tx.success();
            } catch (InterruptedException e) {
                e.printStackTrace();
                System.exit(-1);
            }
        }
    }

    static class RuleApplicationTask extends Thread {
        int id;
        GraphDatabaseService graph;
        BlockingQueue<Rule> abstractRuleQueue;
        Thread ruleReader;
        Context context;

        RuleApplicationTask(int id,
                            GraphDatabaseService graph,
                            BlockingQueue<Rule> abstractRuleQueue,
                            Context context,
                            Thread ruleReader) {
            super("RuleApplication-" + id);
            this.id = id;
            this.abstractRuleQueue = abstractRuleQueue;
            this.graph = graph;
            this.context = context;
            this.ruleReader = ruleReader;
            start();
        }

        @Override
        public void run() {
            try(Transaction tx = graph.beginTx()) {
                while ((ruleReader.isAlive() || !abstractRuleQueue.isEmpty())
                        && context.predictionMapSize() < Settings.SUGGESTION_CAP) {
                    Template abstractRule = (Template) abstractRuleQueue.poll();
                    if (abstractRule != null) {
                        context.addAppliedRule(abstractRule);
                        abstractRule.applyRule(graph, context);
                    }
                }
                tx.success();
            } catch (Exception e) {
                e.printStackTrace();
                System.exit(-1);
            }
        }
    }

    static class RuleReader extends Thread {
        BlockingQueue<Rule> abstractRuleQueue;
        Context context;
        File file;
        int id;

        RuleReader(int id, File file, BlockingQueue<Rule> abstractRuleQueue, Context context) {
            super("RuleReader-" + id);
            this.id = id;
            this.file = file;
            this.abstractRuleQueue = abstractRuleQueue;
            this.context = context;
            start();
        }

        @Override
        public void run() {
            try (LineIterator l = FileUtils.lineIterator(file)) {
                while(l.hasNext()) {
                    String line = l.nextLine();
                    if(line.startsWith("ABS: ")) {
                        int index = Integer.parseInt(line.split("ABS: ")[1].split("\t")[0]);
                        Template rule = (Template) context.getRule(index);
                        if(rule == null) {
                            rule = new Template(line.split("ABS: ")[1]);
                        }
                        if(!rule.head.predicate.equals(Settings.TARGET))
                            continue;
                        if(!rule.isClosed()) {
                            String insRuleLine = l.nextLine();
                            for (String s : insRuleLine.split("\t")) {
                                SimpleInsRule insRule = new SimpleInsRule(rule, s);
                                if(!ValidRuleQuality.overfitting(insRule)) {
                                    rule.insRules.add(insRule);
                                }
                            }
                            if(rule.insRules.isEmpty())
                                continue;
                        } else {
                            String[] words = line.split("ABS: ")[1].split("\t");
                            rule.stats.setStandardConf(Double.parseDouble(words[3]));
                            rule.stats.setSmoothedConf(Double.parseDouble(words[4]));
                            rule.stats.setPcaConf(Double.parseDouble(words[5]));
                            rule.stats.setApcaConf(Double.parseDouble(words[6]));
                            rule.stats.setHeadCoverage(Double.parseDouble(words[7]));
                            rule.stats.setValidPrecision(Double.parseDouble(words[8]));
                            if(ValidRuleQuality.overfitting(rule))
                                continue;
                        }
                        abstractRuleQueue.put(rule);
                    }
                }
            } catch (Exception e) {
                e.printStackTrace();
                System.exit(-1);
            }
        }
    }

    static class RuleWriter extends Thread {
        Thread[] threads;
        File file;
        BlockingQueue<String> contentQueue;
        boolean append;
        int id;

        public RuleWriter(int id, Thread[] threads, File file, BlockingQueue<String> contents, boolean append) {
            super("RuleWriter-" + id);
            this.append = append;
            this.id = id;
            this.file = file;
            this.contentQueue = contents;
            this.threads = threads;
            start();
        }

        @Override
        public void run() {
            boolean stop = false;
            try(PrintWriter writer = new PrintWriter(new FileWriter(file, append))) {
                while(!stop || !contentQueue.isEmpty()) {
                    String line = contentQueue.poll();
                    if(line != null) {
                        writer.println(line);
                    }
                    boolean flag = true;
                    for (Thread producer : threads) {
                        if (producer.isAlive()) {
                            flag = false;
                            break;
                        }
                    }
                    if(flag) stop = true;
                }
            } catch (IOException e) {
                e.printStackTrace();
                System.exit(-1);
            }
        }
    }

}
