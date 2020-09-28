package uk.ac.ncl.graph;

import com.google.common.collect.Multimap;
import com.google.common.collect.MultimapBuilder;
import org.neo4j.graphdb.*;
import uk.ac.ncl.Settings;
import uk.ac.ncl.core.GlobalTimer;
import uk.ac.ncl.core.GraphOps;
import uk.ac.ncl.structure.*;
import uk.ac.ncl.structure.Package;
import uk.ac.ncl.utils.Helpers;
import uk.ac.ncl.utils.Logger;

import java.text.DecimalFormat;
import java.text.MessageFormat;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingDeque;

public class InMemoryGraph {
    public Set<InMemoryRel> relationships = new HashSet<>();
    public Set<String> relationshipTypes = new HashSet<>();
    public Set<InMemoryNode> nodes = new HashSet<>();

    private Multimap<String, InMemoryRel> type2Rel = MultimapBuilder.hashKeys().hashSetValues().build();
    private Multimap<InMemoryNode, InMemoryRel> head2Rel = MultimapBuilder.hashKeys().hashSetValues().build();
    private Multimap<InMemoryNode, InMemoryRel> tail2Rel = MultimapBuilder.hashKeys().hashSetValues().build();
    private Map<InMemoryNode, Multimap<String, InMemoryRel>> headType2Rel = new HashMap<>();
    private Map<InMemoryNode, Multimap<String, InMemoryRel>> tailType2Rel = new HashMap<>();
    private Map<Long, InMemoryNode> id2Node = new HashMap<>();
    private Map<String, InMemoryNode> name2Node = new HashMap<>();

    final private TripleSet tripleSet;

    public InMemoryGraph(GraphDatabaseService database, TripleSet tripleSet, int range) {
        this.tripleSet = tripleSet;
        try(Transaction tx = database.beginTx()) {
            Set<Long> allTargetNodes = new HashSet<>();
            tripleSet.trainPairs.forEach(p -> {
                allTargetNodes.add(p.subId);
                allTargetNodes.add(p.objId);
            });

            tripleSet.validPairs.forEach(p -> {
                allTargetNodes.add(p.subId);
                allTargetNodes.add(p.objId);
            });

            tripleSet.testPairs.forEach(p -> {
                allTargetNodes.add(p.subId);
                allTargetNodes.add(p.objId);
            });

            int current = 0;
            Set<Long> currentTargetNodes= new HashSet<>(allTargetNodes);
            while(current < range) {
                Set<Long> nextTargetNodes = new HashSet<>();
                for (Long n : currentTargetNodes) {
                    Node neo4jNode = database.getNodeById(n);
                    InMemoryNode node = new InMemoryNode(neo4jNode.getId(), GraphOps.readNeo4jProperty(neo4jNode));

                    nodes.add(node);
                    id2Node.put(node.getId(), node);
                    name2Node.put(node.getName(), node);

                    for (Relationship rel : neo4jNode.getRelationships(Direction.BOTH)) {
                        Pair p = new Pair(rel.getStartNodeId(), rel.getEndNodeId());
                        if(tripleSet.testPairs.contains(p) || tripleSet.validPairs.contains(p))
                            if(rel.getType().name().equals(Settings.TARGET))
                                continue;

                        InMemoryNode startNode = new InMemoryNode(rel.getStartNodeId(), GraphOps.readNeo4jProperty(rel.getStartNode()));
                        InMemoryNode endNode = new InMemoryNode(rel.getEndNodeId(), GraphOps.readNeo4jProperty(rel.getEndNode()));

                        InMemoryRel r = new InMemoryRel(startNode, endNode, rel.getType().name());
                        relationships.add(r);
                        relationshipTypes.add(r.getType());

                        type2Rel.put(r.getType(), r);
                        head2Rel.put(r.getStartNode(), r);
                        tail2Rel.put(r.getEndNode(), r);

                        if(!headType2Rel.containsKey(r.getStartNode()))
                            headType2Rel.put(r.getStartNode(), MultimapBuilder.hashKeys().hashSetValues().build());
                        headType2Rel.get(r.getStartNode()).put(r.getType(), r);

                        if(!tailType2Rel.containsKey(r.getEndNode()))
                            tailType2Rel.put(r.getEndNode(), MultimapBuilder.hashKeys().hashSetValues().build());
                        tailType2Rel.get(r.getEndNode()).put(r.getType(), r);

                        Node other = rel.getOtherNode(neo4jNode);
                        if(!allTargetNodes.contains(other.getId())) {
                            allTargetNodes.add(other.getId());
                            nextTargetNodes.add(other.getId());
                        }
                    }
                }
                currentTargetNodes = nextTargetNodes;
                current++;
            }
            tx.success();
        }
    }

    public Set<Pair> groundRules(Rule pattern) {
        Set<Pair> pairs = new HashSet<>();
        Flag stop = new Flag();

        Collection<InMemoryRel> currentRelationships = type2Rel.get(pattern.getBodyAtom(0).getBasePredicate());
        for (InMemoryRel relationship : currentRelationships) {
            if(stop.flag) break;
            Path currentPath = new Path(relationship, pattern.getBodyAtom(0).direction);
            DFSGrounding(pattern, currentPath, pairs, stop);
        }
        return pairs;
    }

    private void DFSGrounding(Rule pattern, Path path, Set<Pair> pairs, Flag stop) {
        if(path.length() == pattern.length()) {
            Pair current;
            if(pattern.closed) {
                current = pattern.isFromSubject() ?
                        new Pair(path.getStartNode().getId(), path.getEndNode().getId()) :
                        new Pair(path.getEndNode().getId(), path.getStartNode().getId());
            } else {
                InstantiatedRule insPattern = (InstantiatedRule) pattern;
                if(insPattern.type == 2) {
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
            String nextType = pattern.getBodyAtom(path.length()).predicate;
            for (InMemoryRel relationship : getRelationships(path.getEndNode(), nextDirection, nextType)) {
                if(!path.nodes.contains(relationship.getOtherNode(path.getEndNode()))) {
                    Path currentPath = new Path(path, relationship);
                    DFSGrounding(pattern, currentPath, pairs, stop);
                    if (stop.flag) break;
                }
            }
        }
    }

    public void ruleApplication(List<Rule> rules) {
        long s = System.currentTimeMillis();

        LinkedBlockingDeque<Package> inputQueue = new LinkedBlockingDeque<>();
        Package.resetIndex();
        rules.forEach(r -> inputQueue.add(Package.create(r)));
        ConcurrentHashMap<Integer, Package> outputQueue = new ConcurrentHashMap<>();

        Dispatcher dispatcher = new Dispatcher(inputQueue, outputQueue, tripleSet);
        RuleApplier[] appliers = new RuleApplier[Settings.THREAD_NUMBER];
        for (int i = 0; i < appliers.length; i++) {
            appliers[i] = new RuleApplier(i, dispatcher, outputQueue, inputQueue, tripleSet);
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

    public Path samplePath(Pair pair, String target, int depth) {
//        Random rand = new Random();
//
//        InMemoryNode startNode = id2Node.get(pair.subId);
//        InMemoryNode endNode = id2Node.get(pair.objId);
//
//
//
//        int currentDepth = 0;
//        Path path;
//        while(currentDepth < depth) {
//            List<InMemoryRel> rels = getRelationships(startNode, Direction.BOTH);
//            InMemoryRel rel = rels.get(rand.nextInt(rels.size()));
//
//
//            currentDepth++;
//        }
        return null;
    }

    private List<InMemoryRel> getRelationships(InMemoryNode node, Direction d, String type) {
        if(d.equals(Direction.OUTGOING)) {
            if(headType2Rel.get(node) != null)
                return new ArrayList<>(headType2Rel.get(node).get(type));
            else
                return new ArrayList<>();
        }
        else if(d.equals(Direction.INCOMING)) {
            if (tailType2Rel.get(node) != null)
                return new ArrayList<>(tailType2Rel.get(node).get(type));
            else
                return new ArrayList<>();
        } else if(d.equals(Direction.BOTH)) {
            List<InMemoryRel> results = new ArrayList<>();
            if(headType2Rel.get(node) != null)
                results.addAll(headType2Rel.get(node).get(type));
            if(tailType2Rel.get(node) != null)
                results.addAll(tailType2Rel.get(node).get(type));
            return results;
        }
        else{
            System.err.println("Error: Invalid direction type.");
            System.exit(-1);
            return null;
        }
    }

    private List<InMemoryRel> getRelationships(InMemoryNode node, Direction d) {
        List<InMemoryRel> results = new ArrayList<>();
        if(d.equals(Direction.OUTGOING)) {
            results.addAll(head2Rel.get(node));
        } else if(d.equals(Direction.INCOMING)) {
            results.addAll(tail2Rel.get(node));
        } else if(d.equals(Direction.BOTH)) {
            results.addAll(head2Rel.get(node));
            results.addAll(tail2Rel.get(node));
        } else {
            System.err.println("Error: Invalid direction type.");
            System.exit(-1);
        }
        return results;
    }

    public static class Flag {
        public boolean flag;
        public Flag() {
            flag = false;
        }
    }

    class RuleApplier extends Thread {
        Dispatcher dispatcher;
        ConcurrentHashMap<Integer, Package> outputQueue;
        LinkedBlockingDeque<Package> inputQueue;
        TripleSet tripleSet;

        public RuleApplier(int id, Dispatcher dispatcher
                , ConcurrentHashMap<Integer, Package> outputQueue
                , LinkedBlockingDeque<Package> inputQueue
                , TripleSet tripleSet) {
            super("RuleApplier-" + id);
            this.dispatcher = dispatcher;
            this.outputQueue = outputQueue;
            this.inputQueue = inputQueue;
            this.tripleSet = tripleSet;
            start();
        }

        @Override
        public void run() {
            try {
                Thread.sleep(300);
                while (dispatcher.isAlive() && !inputQueue.isEmpty()) {
                    if (outputQueue.size() < 3000) {
                        Package p = inputQueue.poll();
                        if (p != null) {
                            p.candidates = groundRules(p.rule);
                            outputQueue.put(p.id, p);
                        }
                    }
                }
            } catch (InterruptedException e) {
                e.printStackTrace();
                System.exit(-1);
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
                    if(!p.candidates.isEmpty()) {
                        tripleSet.updateTestCases(p);

                        System.out.println(p.rule + "\t" + current);
                    }
                    converge = tripleSet.converge();
                }

//                if(current % 5000 == 0 && current != previous) {
//                    Logger.println("# Visited " + current + " Rules | Coverage: " + f.format(tripleSet.coverage));
//                }
                previous = current;

                boolean empty = inputQueue.isEmpty() && outputQueue.isEmpty();
                if(converge) break;
                else if(empty && current == allRules) break;
            }

            Logger.println(MessageFormat.format("# Visited/All Rules: {0}/{1} | Coverage: {2}",
                    current, allRules, f.format(tripleSet.coverage)));
        }
    }

    static class Path {
        public List<InMemoryRel> relationships = new ArrayList<>();
        public List<Direction> directions = new ArrayList<>();
        public List<InMemoryNode> nodes = new ArrayList<>();

        InMemoryRel lastRelationship;

        public Path(Path base, InMemoryRel added) {
            relationships.addAll(base.relationships);
            nodes.addAll(base.nodes);
            directions.addAll(base.directions);

            InMemoryNode endNode = getEndNode();
            if(added.getStartNode().equals(endNode)) directions.add(Direction.OUTGOING);
            else directions.add(Direction.INCOMING);

            relationships.add(added);
            nodes.add(added.getOtherNode(endNode));

            lastRelationship = added;
        }

        public Path(InMemoryRel l, Direction d) {
            relationships.add(l);
            directions.add(d);
            if(d.equals(Direction.OUTGOING)) {
                nodes.add(l.getStartNode());
                nodes.add(l.getEndNode());
            } else {
                nodes.add(l.getEndNode());
                nodes.add(l.getStartNode());
            }
        }

        public int length() {
            return relationships.size();
        }

        public InMemoryNode getEndNode() {
            return nodes.get(nodes.size() - 1);
        }

        public InMemoryNode getStartNode() {
            return nodes.get(0);
        }

        @Override
        public String toString() {
            return relationships.toString();
        }
    }



}
