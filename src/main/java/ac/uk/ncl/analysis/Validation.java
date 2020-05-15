package ac.uk.ncl.analysis;

import ac.uk.ncl.Settings;
import ac.uk.ncl.core.GraphOps;
import ac.uk.ncl.structure.Instance;
import ac.uk.ncl.utils.Helpers;
import ac.uk.ncl.utils.IO;
import ac.uk.ncl.utils.Logger;
import ac.uk.ncl.utils.MathUtils;
import org.json.JSONArray;
import org.json.JSONObject;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Transaction;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.text.DecimalFormat;
import java.text.MessageFormat;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

public class Validation {

    public static File validFile;
    public static int currentARS = 0;
    public static double currentMRR = 0;

    public static void validFileInit(File home, String fileName) {
        validFile = new File(home, fileName);
        try( PrintWriter pw = new PrintWriter(validFile) ) {
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void record(String msg) {
        try(PrintWriter writer = new PrintWriter(new FileWriter(validFile, true), true)) {
            writer.print(msg);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void prepareFiles(File config) {
        JSONObject args = Helpers.buildJSONObject(config);
        File home = new File(args.getString("home"));
        Logger.init(new File(home, "file_log.txt"), false);
        GraphDatabaseService graph = IO.loadGraph(new File(home, args.getString("graph_file")));

//        Settings.SPLIT_RATIO = Helpers.readSetting(args, "split_ratio", Settings.SPLIT_RATIO);
        Settings.RANDOMLY_SELECTED_RELATIONS = args.getInt("randomly_selected_relations");

        List<String> targets;
        JSONArray array = args.getJSONArray("target_relation");
        if(!array.isEmpty())
            targets = StreamSupport.stream(array.spliterator(), false).map(obj -> (String) obj).collect(Collectors.toList());
        else {
            try(Transaction tx = graph.beginTx()) {
                targets = graph.getAllRelationshipTypes().stream().map(RelationshipType::name).collect(Collectors.toList());
                if(Settings.RANDOMLY_SELECTED_RELATIONS != 0 && Settings.RANDOMLY_SELECTED_RELATIONS < targets.size()) {
                    targets = targets.stream().filter( target -> GraphOps.getRelationshipsAPI(graph, target).size() > 300 && GraphOps.getRelationshipsAPI(graph, target).size() < 700).collect(Collectors.toList());
                    Collections.shuffle(targets);
                    targets = targets.subList(0, Settings.RANDOMLY_SELECTED_RELATIONS);
                }
                tx.success(); }
        }

        File resultHome = new File(home, "results");
        resultHome.mkdir();
        Helpers.cleanDirectories(resultHome);
        try(Transaction tx = graph.beginTx()) {
            targets.forEach( target -> {
                System.out.println("# Start Creating Training/Testing Files for Target: " + target);

                File targetHome = new File(resultHome, target.replaceFirst(":", "_"));
                targetHome.mkdir();
                File trainFile = new File(targetHome, "train.txt");
                File testFile = new File(targetHome, "test.txt");
                List<Instance> instances = GraphOps.getRelationshipsAPI(graph, target)
                        .stream().map(Instance::new).collect(Collectors.toList());
                Collections.shuffle(instances);
//                int trainSize = (int) (instances.size() * Settings.SPLIT_RATIO);
                int trainSize = 0;
                List<Instance> train = instances.subList(0, trainSize);
                List<Instance> test = instances.subList(trainSize, instances.size());

                IO.writeInstance(graph, trainFile, train);
                IO.writeInstance(graph, testFile, test);

                System.out.println(MessageFormat.format("# Split Ratio: {0} | Train Instances: {1} | Test Instances: {2}\n",
                        0,
                        train.size(),
                        test.size()));
            });
            tx.success();
        }
    }

    /**
     * Abstract Rule Size HeatMap Experiment.
     * @param config
     * @param pilot
     * @param attempts
     */
    public static void validateExp1(File config, boolean pilot, int attempts) {
        DecimalFormat format = new DecimalFormat("####.####");
        double[] SATs;
        int[] BSs;

        if(pilot) SATs = new double[]{0.5,0.8,0.99};
        else SATs = new double[]{0.5,0.6,0.7,0.8,0.9,0.99,0.999};
        if(pilot) BSs = new int[]{2500, 30000, 90000};
        else BSs = new int[]{2500, 5000, 10000, 30000, 60000, 90000, 120000};

        JSONObject args = Helpers.buildJSONObject(config);
        File home = new File(args.getString("home"));
        File resultsHome = new File(home, "results");
        validFileInit(home, "valid_exp1.txt");

        int totalTests, count = 0;
        totalTests = SATs.length * BSs.length * resultsHome.listFiles().length * attempts;
        
//        LegacyGPFL system = new LegacyGPFL(config);
        for (File file : resultsHome.listFiles()) {
            record("Target: " + file.getName().replaceFirst("concept_", "concept:") + "\n");
            for (double sat : SATs) {
                Settings.SATURATION = sat;
                for (int bSs : BSs) {
                    int[] ars = new int[attempts];
                    double[] mrrs = new double[attempts];

                    Settings.BATCH_SIZE = bSs;
                    for (int i = 0; i < attempts; i++) {
                        System.out.println(MessageFormat.format("\n# ({0}/{1}) VALIDATE: SAT = {2} | BS = {3}",
                                count++, totalTests, sat, bSs));
                        record(sat + "\t" + bSs + "\t");
//                        system.run(file);
                        ars[i] = currentARS;
                        mrrs[i] = currentMRR;
                    }

                    record(format.format(MathUtils.arrayMean(ars)) + "\t" +
                            format.format(MathUtils.STDEV(ars)) + "\t" +
                            format.format(MathUtils.arrayMean(mrrs)) + "\t" +
                            format.format(MathUtils.STDEV(mrrs)) + "\n");
                }
            }
            record("\n");
        }
    }

    public static void validateExp2(File config, boolean pilot, int attempts) {
        DecimalFormat format = new DecimalFormat("####.####");
        int[] sizes;

        if(pilot) sizes = new int[]{200, 600, 1200};
        else sizes = new int[]{200, 400, 600, 800, 1000, 1200};

        JSONObject args = Helpers.buildJSONObject(config);
        File home = new File(args.getString("home"));
        File resultsHome = new File(home, "results");
        validFileInit(home, "valid_exp2.txt");

        int totalTests, count = 0;
        totalTests = sizes.length * resultsHome.listFiles().length * attempts * 2;

//        LegacyGPFL system = new LegacyGPFL(config);
        for (File file : resultsHome.listFiles()) {
            record("Target: " + file.getName().replaceFirst("concept_", "concept:") + "\n");
            for (int size : sizes) {
                Settings.TOP_ABS_RULES = size;
                for(int j = 0; j < 2; j++) {
//                    Settings.USE_RANDOM_RULE_SAMPLE = j == 1;
                    String header = j == 1 ? "random" : "sorted";
                    double[] mrrs = new double[attempts];
                    for (int i = 0; i < attempts; i++) {
                        System.out.println(MessageFormat.format("\n# ({0}/{1}) VALIDATE: Abstract Rule Size: {2} | Mode: {3}",
                                count++, totalTests, size, header));
                        record(header + "\t" + size + "\t");
//                        system.run(file);
                        mrrs[i] = currentMRR;
                    }
                    record(format.format(MathUtils.arrayMean(mrrs)) + "\t" +
                            format.format(MathUtils.STDEV(mrrs)) + "\n");
                }
            }
            record("\n");
        }
    }
}
