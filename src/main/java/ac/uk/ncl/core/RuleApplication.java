package ac.uk.ncl.core;

import ac.uk.ncl.Settings;
import ac.uk.ncl.structure.CountedSet;
import ac.uk.ncl.structure.InstantiatedRule;
import ac.uk.ncl.structure.Pair;
import ac.uk.ncl.structure.Rule;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Transaction;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class RuleApplication {
    public static void multiThreadApplication(BlockingQueue<Rule> ruleQueue, GraphDatabaseService graph, Context context) {
        context.initPredictionMap();
        ExecutorService service = Executors.newFixedThreadPool(Settings.THREAD_NUMBER);
        for (int i = 0; i < Settings.THREAD_NUMBER; i++) {
            service.submit(new RuleConsumer(i, ruleQueue, graph, context));
        }
        try {
            service.shutdown();
            service.awaitTermination(24L, TimeUnit.HOURS);
        } catch (InterruptedException e) {
            e.printStackTrace();
            System.exit(-1);
        }
    }

    static class RuleConsumer implements Runnable {
        BlockingQueue<Rule> ruleQueue;
        GraphDatabaseService graph;
        Context context;
        int id;

        RuleConsumer(int id, BlockingQueue<Rule> ruleQueue, GraphDatabaseService graph, Context context) {
            Thread.currentThread().setName("Application-RuleConsumer-" + id);
            this.id = id;
            this.ruleQueue = ruleQueue;
            this.graph = graph;
            this.context = context;
        }

        @Override
        public void run() {
            try(Transaction tx = graph.beginTx()) {
                while(!ruleQueue.isEmpty()) {
                    Rule rule = ruleQueue.poll();
                    if(rule != null) {
                        singleRuleApplication(graph, rule, context);
                    }
                }
                tx.success();
            }
        }

        private void singleRuleApplication(GraphDatabaseService graph, Rule rule, Context context) {
            CountedSet<Pair> bodyGroundings = GraphOps.bodyGroundingCoreAPI(graph, rule, true, () -> false);
            Set<Long> originals = new HashSet<>();
            for (Pair bodyGrounding : bodyGroundings) {
                originals.add(bodyGrounding.subId);
            }

            if(rule instanceof InstantiatedRule) {
                for (Long original : originals) {
                    Pair pair = rule.isFromSubject() ? new Pair(original, rule.getHeadAnchoring()) : new Pair(rule.getHeadAnchoring(), original);
                    if(!pair.isSelfloop()) {
                        context.putInPredictionMap(pair, rule);
                    }
                }
            } else {
                applyClosedRule(rule, bodyGroundings, context);
            }
        }

        private void applyClosedRule(Rule rule, CountedSet<Pair> bodyGroundings, Context context) {
            for (Pair grounding : bodyGroundings) {
                Pair pair = rule.isFromSubject() ? grounding : new Pair(grounding.objId, grounding.subId);
                if(!pair.isSelfloop()) {
                    context.putInPredictionMap(grounding, rule);
                }
            }
        }
    }


}
