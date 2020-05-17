import uk.ac.ncl.core.Engine;
import uk.ac.ncl.core.Evaluator;
import uk.ac.ncl.model.GPFL;
import uk.ac.ncl.utils.IO;
import org.junit.Test;
import org.neo4j.graphdb.*;

import java.io.File;
import java.util.TreeSet;

public class test {

    @Test
    public void run() {
        File config = new File("data/UWCSE/config.json");
        GPFL system = new GPFL(config, "log");
        system.run();
    }

    @Test
    public void Apply() {
        File config = new File("data/UWCSE/config.json");
        GPFL system = new GPFL(config, "apply_log");
        system.apply();
    }

    @Test
    public void Learn() {
        File config = new File("releaseData/UWCSE/config.json");
        GPFL system = new GPFL(config, "learn_log");
        system.learn();
    }

    @Test
    public void EvaluateGPFL() {
        String home = "reports/FB15K-237/ensemble";
        Evaluator.evalGPFL(home);
    }

    @Test
    public void selectTargets() {
        File config = new File("rqExps/Wikidata/config.json");
        Engine.selectTargets(config);
    }

    @Test
    public void createRandomSplitsFromFiles() {
        File config = new File("fixed/UWCSE-Valid/config.json");
        Engine.createRandomSplitsFromFiles(config);
    }

    @Test
    public void createTriplesFromGraph() {
        File config = new File("fixed/Repotrial-2/config.json");
        Engine.createTripleFileFromGraph(config);
    }

    @Test
    public void createRandomSplitsFromGraph() {
        File config = new File("reports/Repotrial/Repotrial-2/config.json");
        Engine.createRandomSplitsFromGraph(config);
    }

    @Test
    public void guidedCreateAnnotatedFilesFromGraph() {
        File config = new File("reports/Repotrial/Repotrial-1/config.json");
        Engine.createGuidedRandomSplitsFromGraph(config);
    }

    @Test
    public void sampleTargetTriples() {
        File config = new File("fixed/FB15K-10/config.json");
        Engine.sampleTargets(config, 20);
        System.out.println();
        Engine.buildGraph(config.getParent());
    }

    @Test
    public void buildGraph() {
        Engine.buildGraph("ruleQualityExps/Wikidata");
    }

    @Test
    public void buildGraphSingleFile() {
        Engine.buildGraphSingleFile("random/WN18RR");
    }

    @Test
    public void readGraphInfo() {
        String home = "reports/Repotrial/Repotrial-2";
        GraphDatabaseService graph = IO.loadGraph(new File(home, "databases/graph.db"));
        TreeSet<String> types = new TreeSet<>();
        try(Transaction tx = graph.beginTx()) {
            for (RelationshipType type : graph.getAllRelationshipTypes()) {
                types.add(type.name());
            }
            tx.success();
        }
        System.out.println("\n# Relationship Types:");
        types.forEach(System.out::println);
        Runtime.getRuntime().addShutdownHook(new Thread(graph::shutdown));
    }

    @Test
    public void orderGPFLRules() {
        File config = new File("fixed/UWCSE/config.json");
        IO.orderRules(config);
    }

    @Test
    public void graphAndFileVerification() {
        File config = new File("fixed/UWCSE/config.json");
        Engine.verifyGraph(config.getParent());
    }

    @Test
    public void EvaluateAnyBURL() {
        String home = "AnyBURL-Pred/UWCSE";
        Evaluator.evalAnyBURL(home);
    }

    @Test
    public void orderInsRules() {
        IO.orderRuleIndexFile(new File("rqExps/FB15K-237/insRule.txt"));
    }

    @Test
    public void refineRules() {
        File config = new File("data/UWCSE/config.json");
        IO.filterNonOverfittingRules(config);
    }

    @Test
    public void debug() {
        File config = new File("fixed/FB15K-237/config.json");
        GraphDatabaseService graph = IO.loadGraph(new File(config.getParent(), "databases/graph.db"));
        try(Transaction tx = graph.beginTx()) {
            Node s = graph.getNodeById(843);
            Node e = graph.getNodeById(8541);

            String type = "/award/award_winning_work/awards_won./award/award_honor/honored_for";
            System.out.println("Outgoing: ");
            s.getRelationships(RelationshipType.withName(type), Direction.OUTGOING).forEach(rel -> {
                System.out.println(rel.getStartNode().getProperty("name") + "\t" + type + "\t" + rel.getEndNode().getProperty("name"));
            });
            System.out.println("Incoming: ");
            s.getRelationships(RelationshipType.withName(type), Direction.INCOMING).forEach(rel -> {
                System.out.println(rel.getStartNode().getProperty("name") + "\t" + type + "\t" + rel.getEndNode().getProperty("name"));
            });

            tx.success();
        }
    }
}
