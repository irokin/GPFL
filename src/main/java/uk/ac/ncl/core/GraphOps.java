package uk.ac.ncl.core;

import uk.ac.ncl.Settings;
import uk.ac.ncl.structure.*;
import uk.ac.ncl.utils.Helpers;
import uk.ac.ncl.utils.IO;
import uk.ac.ncl.utils.MathUtils;
import com.google.common.collect.Lists;
import com.google.common.collect.Multimap;
import com.google.common.collect.Sets;
import org.neo4j.graphdb.*;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.graphdb.traversal.*;
import uk.ac.ncl.structure.*;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.text.DecimalFormat;
import java.text.MessageFormat;
import java.util.*;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

public class GraphOps {

    public static Map<String, Long> ruleGraphIndexing = new HashMap<>();

    public static void writeToRuleGraph(GraphDatabaseService dataGraph, GraphDatabaseService ruleGraph, Multimap<Pair, Rule> verifications) {
        DecimalFormat format = new DecimalFormat("###.####");
        try(Transaction tx = ruleGraph.beginTx()) {
            verifications.keySet().forEach( prediction -> {
                Node startNode = getRuleGraphNode(ruleGraph, dataGraph.getNodeById(prediction.subId));
                Node endNode = getRuleGraphNode(ruleGraph, dataGraph.getNodeById(prediction.objId));
                int ruleSize = verifications.get(prediction).size();
                double[] aggSupp = new double[ruleSize];
                double[] aggConf = new double[ruleSize];
                double[] aggPred = new double[ruleSize];
                Counter counter = new Counter();
                verifications.get(prediction).forEach( rule -> {
                    String ruleType = "Rule";
                    if(rule.isClosed()) ruleType = "Closed_Abstract_Rule";
                    else if(rule instanceof InstantiatedRule) {
                        if(((InstantiatedRule) rule).getType() == 0) ruleType = "Head_Anchored_Rule";
                        if(((InstantiatedRule) rule).getType() == 2) ruleType = "Both_Anchored_Rule";
                    }
                    Relationship relationship = startNode.createRelationshipTo(endNode, RelationshipType.withName(ruleType));
                    relationship.setProperty("headAtom", rule.head.toString());
                    relationship.setProperty("bodyAtoms", rule.bodyAtoms.stream().map(Atom::toString).toArray(String[]::new));
                    relationship.setProperty("Confidence", format.format(rule.getQuality()));
                    relationship.setProperty("Support", rule.stats.support);
                    relationship.setProperty("Predictions", rule.stats.totalPredictions);
                    aggSupp[counter.count] = rule.stats.support;
                    aggConf[counter.count] = Helpers.formatDouble(format, rule.getQuality());
                    aggPred[counter.count] = rule.stats.totalPredictions;
                    counter.tick();
                });
                Relationship strengthRelationship = startNode.createRelationshipTo(endNode, RelationshipType.withName(Settings.TARGET));
                strengthRelationship.setProperty("Mean Confidence", Helpers.formatDouble(format, MathUtils.arrayMean(aggConf)));
                strengthRelationship.setProperty("Confidences", aggConf);

                strengthRelationship.setProperty("Mean Support", Helpers.formatDouble(format, MathUtils.arrayMean(aggSupp)));
                strengthRelationship.setProperty("Supports", aggSupp);

                strengthRelationship.setProperty("Mean Predictions", Helpers.formatDouble(format, MathUtils.arrayMean(aggPred)));
                strengthRelationship.setProperty("Predictions", aggPred);

                strengthRelationship.setProperty("Rule Counts", ruleSize);
            });
            tx.success();
        }
    }

    public static Node getRuleGraphNode(GraphDatabaseService ruleGraph, Node dataGraphNode) {
        String identifier = Settings.NEO4J_IDENTIFIER;
        String dataGraphGPFLId = (String) dataGraphNode.getProperty(identifier);

        if(ruleGraphIndexing.containsKey(dataGraphGPFLId))
            return ruleGraph.getNodeById(ruleGraphIndexing.get(dataGraphGPFLId));

        Node ruleGraphNode = ruleGraph.createNode();
        dataGraphNode.getAllProperties().forEach(ruleGraphNode::setProperty);
        dataGraphNode.getLabels().forEach(ruleGraphNode::addLabel);
        ruleGraphIndexing.put((String) dataGraphNode.getProperty(identifier), ruleGraphNode.getId());
        return ruleGraphNode;
    }

    public static GraphDatabaseService createEmptyGraph(File home) {
        if(!home.exists()) home.mkdir();
        File databaseFile = new File(home, "databases");
        if(databaseFile.exists()) deleteDirectory(databaseFile);
        return loadGraph(home);
    }

    private static boolean deleteDirectory(File file) {
        File[] contents = file.listFiles();
        if(contents != null) {
            for (File content : contents) {
                deleteDirectory(content);
            }
        }
        return file.delete();
    }

    public static GraphDatabaseService loadGraph(File home) {
        GraphDatabaseService graph = new GraphDatabaseFactory()
                .newEmbeddedDatabase(new File(home, "databases/graph.db"));
        Runtime.getRuntime().addShutdownHook(new Thread(graph::shutdown));
        return graph;
    }

    public static Set<Relationship> getRelationshipsAPI(GraphDatabaseService graph, String relationshipName) {
        Set<Relationship> relationships = new HashSet<>();
        try(Transaction tx = graph.beginTx()) {
            for (Relationship relationship : graph.getAllRelationships()) {
                if(relationship.getType().name().equals(relationshipName)) relationships.add(relationship);
            }
            tx.success();
        }
        return relationships;
    }

    public static Set<Relationship> getRelationshipsQuery(GraphDatabaseService graph, String relationshipName) {
        String query = MessageFormat.format("Match p=()-[:{0}]->() Return p", relationshipName);
        Set<Relationship> relationships = new HashSet<>();
        try(Transaction tx = graph.beginTx()) {
            graph.execute(query).columnAs("p").forEachRemaining( value -> {
                Path path = (Path) value;
                relationships.add(path.lastRelationship());
            });
            tx.success();
        }
        return relationships;
    }

    public static void removeRelationshipAPI(GraphDatabaseService graph, Set<Pair> pairs) {
        try(Transaction tx = graph.beginTx()) {
            pairs.forEach(pair -> pair.rel.delete());
            tx.success();
        }
    }

    public static void removeRelationshipQuery(GraphDatabaseService graph, List<Instance> instances) {
        try( Transaction tx = graph.beginTx()) {
            for( Instance instance : instances ) {
                graph.execute(MessageFormat.format("match (a)-[r:{0}]->(b)\n" +
                                "where id(a)={1} and id(b)={2}\n" +
                                "delete r"
                        , instance.type.name()
                        , String.valueOf(instance.startNodeId)
                        , String.valueOf(instance.endNodeId)));
            }
            tx.success();
        }
    }

    public static List<Instance> addRelationshipAPI(GraphDatabaseService graph, List<Instance> instances, File out) {
        List<Instance> newInstances = new ArrayList<>();
        try(Transaction tx = graph.beginTx()) {
            instances.forEach( instance -> {
                Node startNode = graph.getNodeById(instance.startNodeId);
                Node endNode = graph.getNodeById(instance.endNodeId);
                RelationshipType type = instance.type;
                newInstances.add(new Instance(startNode.createRelationshipTo(endNode, type)));
            });
            tx.success();
        }
        IO.writeInstance(graph, out, newInstances);
        return newInstances;
    }

    public static void addRelationshipAPI(GraphDatabaseService graph, Set<Pair> pairs, File out) {
        try(Transaction tx = graph.beginTx()) {
            try(PrintWriter writer = new PrintWriter(new FileWriter(out, true))) {
                for (Pair pair : pairs) {
                    Node startNode = graph.getNodeById(pair.subId);
                    Node endNode = graph.getNodeById(pair.objId);
                    RelationshipType type = pair.type;
                    Relationship rel = startNode.createRelationshipTo(endNode, type);
                    String[] words = new String[]{String.valueOf(rel.getId())
                            , (String) startNode.getProperty(Settings.NEO4J_IDENTIFIER)
                            , type.name()
                            , (String) endNode.getProperty(Settings.NEO4J_IDENTIFIER)};
                    writer.println(String.join("\t", words));
                }
            }
            tx.success();
        } catch (IOException e) {
            e.printStackTrace();
            System.exit(-1);
        }
    }

    public static List<Instance> addRelationshipQuery(GraphDatabaseService graph, List<Instance> instances) {
        List<Instance> relationships = new ArrayList<>();
        try(Transaction tx = graph.beginTx()) {
            for(Instance instance : instances) {
                relationships = graph.execute( MessageFormat.format("match (a)\n"
                                + "match (b)\n"
                                + "where id(a)={0} and id(b)={1}\n"
                                + "merge (a)-[x:{2}]->(b)\n"
                                + "return x"
                        , String.valueOf(instance.startNodeId)
                        , String.valueOf(instance.endNodeId)
                        , instance.type.name())).columnAs("x")
                        .stream().map( relationship -> new Instance((Relationship) relationship)).collect(Collectors.toList());
            }
            tx.success();
        }
        return relationships;
    }

    public static Set<Path> bodyGroundingTraversal(GraphDatabaseService graph, Rule pattern) {
        Set<Path> results = new HashSet<>();
        Atom initialBodyAtom = pattern.bodyAtoms.get(0);

        Set<Relationship> relationships = getRelationshipsAPI(graph, initialBodyAtom.getBasePredicate());
        Set<Node> initialNodes = initialBodyAtom.isInverse()
                ? relationships.stream().map(Relationship::getEndNode).collect(Collectors.toSet())
                : relationships.stream().map(Relationship::getStartNode).collect(Collectors.toSet());

        Traverser traverser = graph.traversalDescription()
                .uniqueness(Uniqueness.NODE_PATH)
                .order(BranchOrderingPolicies.PREORDER_DEPTH_FIRST)
                .expand(typeConstrainedExpander(pattern))
                .evaluator(Evaluators.atDepth(pattern.bodyAtoms.size()))
                .traverse(initialNodes);

        int count = 0;
        for (Path path : traverser) {
            if(count++ >= Settings.LEARN_GROUNDINGS) break;
            results.add(path);
        }
        return results;
    }

//    public static Set<Path> bodyGroundingQuery(GraphDatabaseService graph, Rule pattern) {
//        Set<Path> results = new HashSet<>();
//        StringBuilder query = new StringBuilder("Match p=()");
//        for (int i = 0; i < pattern.bodyAtoms.size(); i++) {
//            Atom atom = pattern.bodyAtoms.get(i);
//            if ( atom.getDirection().equals( Direction.OUTGOING ) )
//                query.append(MessageFormat.format("-[:{0}]->"
//                        , atom.getPredicate()));
//            else
//                query.append(MessageFormat.format("<-[:{0}]-"
//                        , atom.getPredicate().replaceFirst("_", "")));
//            if ( i == pattern.bodyAtoms.size() - 1 ) {
//                int size = Settings.LEARN_GROUNDINGS;
//                if(size == 0) query.append("() return p");
//                else query.append(MessageFormat.format("() return p limit {0}"
//                        , String.valueOf(size)));
//            } else query.append("()");
//        }
//        graph.execute(query.toString()).columnAs("p")
//                .forEachRemaining(t -> results.add((Path) t));
//        return results;
//    }

    public static CountedSet<Pair> bodyGroundingCoreAPI(GraphDatabaseService graph, Rule pattern
            , boolean application, Supplier<Boolean> stoppingCondition) {
        CountedSet<Pair> pairs = new CountedSet<>();
        Flag stop = new Flag();

        boolean checkTail = false;
        if(pattern instanceof InstantiatedRule || pattern instanceof SimpleInsRule) {
            int type = pattern.getType();
            if(type == 1 || type == 2) checkTail = true;
        }

        Set<Relationship> currentRelationships = getRelationshipsAPI(graph, pattern.getBodyAtom(0).getBasePredicate());
        for (Relationship relationship : currentRelationships) {
            if(stop.flag || stoppingCondition.get()) break;
            LocalPath currentPath = new LocalPath(relationship, pattern.getBodyAtom(0).direction);
            DFSGrounding(pattern, currentPath, pairs, stop, checkTail, application, stoppingCondition);
        }

        return pairs;
    }

    private static void DFSGrounding(Rule pattern, LocalPath path, CountedSet<Pair> pairs, Flag stop
            , boolean checkTail, boolean application, Supplier<Boolean> stoppingCondition) {
        if(path.length() >= pattern.length()) {
            if(checkTail && pattern.getTailAnchoring() != path.getEndNode().getId()) return;
            pairs.add(new Pair(path.getStartNode().getId(), path.getEndNode().getId()));
            int groundingCap = application ? Settings.APPLY_GROUNDINGS : Settings.LEARN_GROUNDINGS;
            if(pairs.size() >= groundingCap)
                stop.flag = true;
        }
        else {
            Direction nextDirection = pattern.getBodyAtom(path.length()).direction;
            RelationshipType nextType = RelationshipType.withName(pattern.getBodyAtom(path.length()).predicate);
            for (Relationship relationship : path.getEndNode().getRelationships(nextDirection, nextType)) {
                if(stoppingCondition.get()) break;

                if(!path.nodes.contains(relationship.getOtherNode(path.getEndNode()))) {
                    LocalPath currentPath = new LocalPath(path, relationship);
                    DFSGrounding(pattern, currentPath, pairs, stop, checkTail, application, stoppingCondition);
                    if (stop.flag) break;
                }
            }
        }
    }

    public static Traverser buildStandardTraverser(GraphDatabaseService graph, Pair pair, int randomWalkers){
        Traverser traverser;
        Node startNode = graph.getNodeById(pair.subId);
        Node endNode = graph.getNodeById(pair.objId);
        traverser = graph.traversalDescription()
                .uniqueness(Uniqueness.NODE_PATH)
                .order(BranchingPolicy.PreorderBFS())
                .expand(standardRandomWalker(randomWalkers))
                .evaluator(toDepthNoTrivial(Settings.DEPTH, pair))
                .traverse(startNode, endNode);
        return traverser;
    }

    private static <STATE> PathExpander<STATE> typeConstrainedExpander(Rule pattern) {
        return new PathExpander<STATE>() {
            @Override
            public Iterable<Relationship> expand(Path path, BranchState<STATE> state) {
                int current = path.length();
                RelationshipType nextRelationshipType = pattern.bodyAtoms.get(current).type;
                Direction nextDirection = pattern.bodyAtoms.get(current).direction;
                Iterable<Relationship> result = path.endNode().getRelationships(nextRelationshipType, nextDirection);
                return result;
            }

            @Override
            public PathExpander<STATE> reverse() {
                return null;
            }
        };
    }

    public static List<Path> pathSamplingTraversal(GraphDatabaseService graph, Pair pair, int depth, int randomWalkers) {
        Node startNode = graph.getNodeById(pair.subId);
        Node endNode = graph.getNodeById(pair.objId);
        Traverser traverser = graph.traversalDescription()
                .uniqueness(Uniqueness.NODE_PATH)
                .order(BranchOrderingPolicies.PREORDER_DEPTH_FIRST)
                .expand(standardRandomWalker(randomWalkers))
                .evaluator(toDepthNoTrivial(depth, pair))
                .traverse(startNode, endNode);
        return traverser.stream().collect(Collectors.toList());
    }

    /**
     * For extracting paths from the graph, we need to ensure certain properties about paths:
     * - Must be node-unique, which means a node cannot be visited twice in the path unless it is
     * one of the nodes in the head, thus it is a closed path
     * - We need to ensure there is certain randomness when extracting paths from instances, that is when
     * visit an instance again, the extracted path should not be deterministic. This is enabled by the
     * random walker.
     *
     * @param instance
     * @param depth
     * @param randomWalkers
     * @return
     */
    public static List<LocalPath> pathSamplingCoreAPI(GraphDatabaseService graph, Instance instance, int depth, int randomWalkers) {
        List<LocalPath> paths = new ArrayList<>();

        Node startNode = instance.relationship.getStartNode();
        Node endNode = instance.relationship.getEndNode();

        DFS(paths, new LocalPath(startNode, instance.relationship), 0, depth, randomWalkers);
        DFS(paths, new LocalPath(endNode, instance.relationship), 0, depth, randomWalkers);

        return paths;
    }

    private static void DFS(List<LocalPath> paths, LocalPath path, int currentDepth, int depth, int randomWalkers) {
        if(currentDepth >= depth) return;

        Node endNode = path.getEndNode();
        Predicate<Relationship> evaluator = (relationship)
                -> !path.nodes.contains(relationship.getOtherNode(endNode));

        List<Relationship> relationships = StreamSupport.stream(endNode.getRelationships().spliterator(), false)
                .filter(evaluator).collect(Collectors.toList());

        Random rand = new Random();
        List<Relationship> selected = new ArrayList<>();
        if(relationships.size() > randomWalkers) {
            while (selected.size() < randomWalkers) {
                Relationship select = relationships.get(rand.nextInt(relationships.size()));
                relationships.remove(select);
                selected.add(select);
            }
        } else selected = relationships;

        for (Relationship relationship : selected) {
            LocalPath currentPath = new LocalPath(path, relationship);
            paths.add(currentPath);
            DFS(paths, currentPath, currentDepth + 1, depth, randomWalkers);
        }
    }

    public static PathExpander standardRandomWalker(int randomWalkers) {
        return new PathExpander() {
            @Override
            public Iterable<Relationship> expand(Path path, BranchState state) {
                Set<Relationship> results = Sets.newHashSet();
                List<Relationship> candidates = Lists.newArrayList( path.endNode().getRelationships() );
                if ( candidates.size() < randomWalkers || randomWalkers == 0 ) return candidates;

                Random rand = new Random();
                for ( int i = 0; i < randomWalkers; i++ ) {
                    int choice = rand.nextInt( candidates.size() );
                    results.add( candidates.get( choice ) );
                    candidates.remove( choice );
                }

                return results;
            }

            @Override
            public PathExpander reverse() {
                return null;
            }
        };
    }

    public static  PathEvaluator toDepthNoTrivial(final int depth, Pair pair) {
        return new PathEvaluator.Adapter()
        {
            @Override
            public Evaluation evaluate(Path path, BranchState state)
            {
                boolean fromSource = pair.subId == path.startNode().getId();
                boolean closed = pathIsClosed( path, pair );
                boolean hasTargetRelation = false;
                int pathLength = path.length();

                if ( path.lastRelationship() != null ) {
                    Relationship relation = path.lastRelationship();
                    hasTargetRelation = relation.getType().equals(pair.type);
                    if ( pathLength == 1
                            && relation.getStartNodeId() == pair.objId
                            && relation.getEndNodeId() == pair.subId
                            && hasTargetRelation)
                        return Evaluation.INCLUDE_AND_PRUNE;
                }

                if ( pathLength == 0 )
                    return Evaluation.EXCLUDE_AND_CONTINUE;

                if ( pathLength == 1 && hasTargetRelation && closed )
                    return Evaluation.EXCLUDE_AND_PRUNE;

                if ( closed && fromSource )
                    return Evaluation.INCLUDE_AND_PRUNE;
                else if ( closed )
                    return Evaluation.EXCLUDE_AND_PRUNE;

                if (selfloop(path))
                    return Evaluation.EXCLUDE_AND_PRUNE;

                return Evaluation.of( pathLength <= depth, pathLength < depth );
            }
        };
    }

    private static boolean pathIsClosed(Path path, Pair pair) {
        boolean fromSource = path.startNode().getId() == pair.subId;
        if ( fromSource )
            return path.endNode().getId() == pair.objId;
        else
            return path.endNode().getId() == pair.subId;
    }

    private static boolean selfloop(Path path) {
        return path.startNode().equals( path.endNode() ) && path.length() != 0;
    }

    static class Counter {
        int count = 0;
        public void tick() {
            count++;
        }
    }

    static class Flag {
        boolean flag;
        public Flag() {
            flag = false;
        }
    }
}
