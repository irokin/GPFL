package ac.uk.ncl.analysis;

import ac.uk.ncl.Settings;
import ac.uk.ncl.core.GraphOps;
import ac.uk.ncl.structure.Instance;
import ac.uk.ncl.utils.Helpers;
import ac.uk.ncl.utils.IO;
import com.google.common.collect.Multimap;
import com.google.common.collect.MultimapBuilder;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.LineIterator;
import org.json.JSONObject;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Transaction;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class RuleComposition {

    public static void analysis(File config) {
        System.out.println("# Start Rule Space Composition Analysis:");

        DecimalFormat f = new DecimalFormat("####.####");
        JSONObject args = Helpers.buildJSONObject(config);
        File home = new File(args.getString("home"));
        Settings.NEO4J_IDENTIFIER = Helpers.readSetting(args, "neo4j_identifier", Settings.NEO4J_IDENTIFIER);
        File resultHome = new File(home, "results");
        GraphDatabaseService graph = GraphOps.loadGraph(home);
        File out = new File(home, "RuleAnalysis.txt");

        System.out.println("# Read Settings from: " + home);

        try(PrintWriter writer = new PrintWriter(out)) {
            try (Transaction tx = graph.beginTx()) {
                for (File targetHome : resultHome.listFiles()) {
                    File verificationFile = new File(targetHome, "verifications.txt");
                    if (!verificationFile.exists()) continue;
                    List<Instance> truth = IO.readInstance(graph, new File(targetHome, "test.txt"));

                    Set<SimpleRule> allRules = new HashSet<>();
                    Set<SimpleRule> correctRules = new HashSet<>();

                    try (LineIterator l = FileUtils.lineIterator(verificationFile)) {
                        boolean correctPrediction = false;
                        while (l.hasNext()) {
                            String line = l.nextLine();
                            if (line.startsWith("(")) {
                                line = line.substring(1, line.length() - 1);
                                String[] terms = line.split(", ");
                                for (Instance instance : truth) {
                                    if (instance.startNodeName.equals(terms[0]) && instance.endNodeName.equals(terms[2])) {
                                        correctPrediction = true;
                                        break;
                                    }
                                }
                            } else if (line.isEmpty()) {
                                correctPrediction = false;
                            } else {
                                String type = line.substring(0, 3);
                                String head = line.substring(4).split(" <- ")[0];
                                String[] body = line.substring(4).split(" <- ")[1].split("\t")[0].split("\\),");
                                for (int i = 0; i < body.length; i++) {
                                    if (i != body.length - 1) body[i] += ")";
                                }
                                SimpleRule rule = new SimpleRule(type, head, body);
                                allRules.add(rule);
                                if (correctPrediction) correctRules.add(rule);
                            }
                        }
                    }

                    String target = targetHome.getName().replaceFirst("concept_", "concept:");
                    System.out.println("# Analyzing Target: " + target + "\n");
                    writer.println("Target: " + target);
                    Multimap<Integer, SimpleRule> ruleByLength = MultimapBuilder.treeKeys().hashSetValues().build();
                    correctRules.forEach(rule -> ruleByLength.put(rule.length, rule));

                    int totalCARCount = 0, totalBARCount = 0, totalHARCount = 0;
                    List<Integer> lengthProportions = new ArrayList<>();
                    for (Integer key : ruleByLength.keySet()) {
                        lengthProportions.add(ruleByLength.get(key).size());
                        writer.print("Length " + key + ": ");
                        int CARCount = 0, BARCount = 0, HARCount = 0;
                        int total = ruleByLength.get(key).size();
                        for (SimpleRule rule : ruleByLength.get(key)) {
                            if(rule.type.equals("CAR")) CARCount++;
                            else if(rule.type.equals("BAR")) BARCount++;
                            else if(rule.type.equals("HAR")) HARCount++;
                        }
                        totalCARCount += CARCount;
                        totalBARCount += BARCount;
                        totalHARCount += HARCount;

                        writer.print(f.format((double) CARCount / total) + "\t" +
                                f.format((double) BARCount / total) + "\t" +
                                f.format((double) HARCount / total) + "\n");
                    }
                    writer.print("All Types: " + f.format((double) totalCARCount / correctRules.size()) + "\t" +
                            f.format((double) totalBARCount / correctRules.size()) + "\t" +
                            f.format((double) totalHARCount / correctRules.size()) + "\n");

                    writer.print("All Length: ");
                    for (int lengthProportion : lengthProportions) {
                        writer.print(f.format((double) lengthProportion / correctRules.size()) + "\t");
                    }
                    writer.println();
                }
                tx.success();
            }
        }  catch (IOException e) {
            e.printStackTrace();
            System.exit(-1);
        }

        System.out.println("# Finished Analyzing: " + resultHome.listFiles().length + " Targets.");
    }

    static class SimpleRule {
        int length;
        String type;
        String head;
        String[] body;

        public SimpleRule(String t, String h, String[] b) {
            type = t;
            head = h;
            body = b;
            length = body.length;
        }

        @Override
        public int hashCode() {
            int code = type.hashCode() + type.hashCode() + head.hashCode();
            for (String s : body)  code += s.hashCode();
            return code;
        }

        @Override
        public boolean equals(Object obj) {
            if(obj instanceof SimpleRule) {
                SimpleRule other = (SimpleRule) obj;
                if(other.head.equals(head) && other.type.equals(type) && other.length == length) {
                    for (int i = 0; i < length; i++) {
                        if(!body[i].equals(other.body[i])) return false;
                    }
                    return true;
                } return false;
            } return false;
        }

        @Override
        public String toString() {
            return type + "\t" + head + " <- " + String.join(", ", body);
        }
    }
}
