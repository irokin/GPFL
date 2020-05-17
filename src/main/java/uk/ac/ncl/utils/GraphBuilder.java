package uk.ac.ncl.utils;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.LineIterator;
import org.neo4j.graphdb.*;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.text.DecimalFormat;
import java.text.MessageFormat;
import java.util.*;
import java.util.stream.Collectors;

public class GraphBuilder {

    private static Map<String, Long> map = new HashMap<>();

    public static GraphDatabaseService populateGraphFromTriples(File graphFile, File trainFile) {
        System.out.println("# GPFL System - Neo4j Graph Database Construction: ");
        GraphDatabaseService graph = createEmptyGraph(graphFile);
        Set<localTriple> trainTriples = new HashSet<>(readTriples(trainFile));

        System.out.println(MessageFormat.format("# Data Stats: Train={0}"
                , trainTriples.size()));
        writeToGraph(graph, trainTriples, true, true);
        writeToSeparateFile(trainTriples, new File(trainFile.getParent(), "annotated_train.txt"));

        DecimalFormat format = new DecimalFormat("####.###");
        try(Transaction tx = graph.beginTx()) {
            long relationshipTypes = graph.getAllRelationshipTypes().stream().count();
            long relationships = graph.getAllRelationships().stream().count();
            long nodes = graph.getAllNodes().stream().count();

            Logger.println(MessageFormat.format("# Relationship Types: {0} | Relationships: {1} " +
                            "| Nodes: {2} | Instance Density: {3} | Degree {4}",
                    relationshipTypes,
                    relationships,
                    nodes,
                    format.format((double) relationships / relationshipTypes),
                    format.format((double) relationships / nodes)), 1);
            tx.success();
        }
        return graph;
    }

    public static GraphDatabaseService populateGraphFromTriples(File graphFile, File trainFile, File validFile, File testFile) {
        System.out.println("# GPFL System - Neo4j Graph Database Construction: ");

        GraphDatabaseService graph = createEmptyGraph(graphFile);
        Set<localTriple> trainTriples = new HashSet<>(readTriples(trainFile));
        Set<localTriple> validTriples = new HashSet<>(readTriples(validFile));
        Set<localTriple> testTriples = new HashSet<>(readTriples(testFile));
        Set<localTriple> completeTriples = new HashSet<>();

        completeTriples.addAll(trainTriples);
        completeTriples.addAll(validTriples);
        completeTriples.addAll(testTriples);

        System.out.println(MessageFormat.format("# Data Stats: Train={0} | Valid={1} | Test={2} | All={3}"
                , trainTriples.size()
                , validTriples.size()
                , testTriples.size()
                , completeTriples.size())
        );

        writeToGraph(graph, trainTriples, true, true);
        writeToGraph(graph, validTriples, true, false);
        writeToGraph(graph, testTriples, true, false);

        writeToSeparateFile(trainTriples, new File(trainFile.getParent(), "annotated_train.txt"));
        writeToSeparateFile(validTriples, new File(validFile.getParent(), "annotated_valid.txt"));
        writeToSeparateFile(testTriples, new File(testFile.getParent(), "annotated_test.txt"));

        DecimalFormat format = new DecimalFormat("####.###");

        try(Transaction tx = graph.beginTx()) {
            long relationshipTypes = graph.getAllRelationshipTypes().stream().count();
            long relationships = graph.getAllRelationships().stream().count();
            long nodes = graph.getAllNodes().stream().count();

            Logger.println(MessageFormat.format("# Relationship Types: {0} | Relationships: {1} " +
                            "| Nodes: {2} | Instance Density: {3} | Degree {4}",
                    relationshipTypes,
                    relationships,
                    nodes,
                    format.format((double) relationships / relationshipTypes),
                    format.format((double) relationships / nodes)), 1);
            tx.success();
        }

        return graph;
    }

    private static void writeToSeparateFile(Set<localTriple> triples, File outFile) {
        try(PrintWriter writer = new PrintWriter(new FileWriter(outFile))) {
            for (localTriple triple : triples)
                writer.println(triple.toFileLine());
        } catch (IOException e) {
            e.printStackTrace();
            System.exit(-1);
        }
    }

    private static void analyzeTripleSets(Set<localTriple> trainTriples, Set<localTriple> validTriples, Set<localTriple> testTriples) {
        Map<String, Integer> trainCounter = createMapCounter(trainTriples);
        Map<String, Integer> validCounter = createMapCounter(validTriples);
        Map<String, Integer> testCounter = createMapCounter(testTriples);

        System.out.println(trainCounter);
        System.out.println(validCounter);
        System.out.println(testCounter);
    }

    private static Map<String, Integer> createMapCounter(Set<localTriple> triples) {
        Map<String, Integer> map = new HashMap<>();
        for (localTriple triple : triples) {
            if(map.containsKey(triple.relation))
                map.put(triple.relation, map.get(triple.relation) + 1);
            else
                map.put(triple.relation, 1);
        }
        return map;
    }

    public static void writeToGraph(GraphDatabaseService graph, Set<localTriple> triples
            , boolean singleProperty, boolean createRelationship) {
        int current = 0;
        List<localTriple> tripleList = new ArrayList<>(triples);
        while(current < triples.size()) {
            try(Transaction tx = graph.beginTx()) {
                for (int i = current; i < triples.size();) {
                    localTriple triple = tripleList.get(i);
                    Node headNode = null, tailNode = null;
                    if (map.containsKey(triple.head)) headNode = graph.getNodeById(map.get(triple.head));
                    if (map.containsKey(triple.tail)) tailNode = graph.getNodeById(map.get(triple.tail));

                    if (headNode == null) {
                        headNode = graph.createNode();
                        if (singleProperty) headNode.setProperty("name", triple.head);
                        else setProperties(triple.headProperties, headNode);
                        if (triple.headLabels.isEmpty()) headNode.addLabel(Label.label("Entity"));
                        else for (String label : triple.headLabels) headNode.addLabel(Label.label(label));
                        map.put(triple.head, headNode.getId());
                    }
                    if (tailNode == null) {
                        tailNode = graph.createNode();
                        if (singleProperty) tailNode.setProperty("name", triple.tail);
                        else setProperties(triple.tailProperties, tailNode);
                        if (triple.tailLabels.isEmpty()) tailNode.addLabel(Label.label("Entity"));
                        else for (String label : triple.tailLabels) tailNode.addLabel(Label.label(label));
                        map.put(triple.tail, tailNode.getId());
                    }

                    triple.headId = headNode.getId();
                    triple.tailId = tailNode.getId();

                    if (createRelationship) {
                        Relationship relationship = headNode.createRelationshipTo(tailNode, RelationshipType.withName(triple.relation));
                        triple.relId = relationship.getId();
                        if (!singleProperty) setProperties(triple.relationProperties, relationship);
                    }

                    i++;
                    current++;
                    if (i % 500000 == 0) {
                        System.out.println("# Created " + current + " Relationships in the Graph.");
                        break;
                    }
                }
                tx.success();
            }
        }
    }

    private static void setProperties(Map<String, Object> propertyMap, Entity entity) {
        for (Map.Entry<String, Object> entry : propertyMap.entrySet()) {
            if(entry.getValue() instanceof Collection) {
                entity.setProperty(entry.getKey(), ((Collection) entry.getValue()).toArray(new String[0]));
            } else
                entity.setProperty(entry.getKey(), entry.getValue());
        }
    }

    public static GraphDatabaseService createEmptyGraph(File graphFile) {
        System.out.println("# Created New Neo4J Graph at: " + graphFile.getAbsolutePath());
        deleteDirectory(graphFile);
        GraphDatabaseService graph = new GraphDatabaseFactory().newEmbeddedDatabase(graphFile);
        Runtime.getRuntime().addShutdownHook(new Thread(graph::shutdown));
        return graph;
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

    public static Set<localTriple> readTriples(File file) {
        Set<localTriple> triples = new HashSet<>();
        if(file.exists()) {
            try (LineIterator l = FileUtils.lineIterator(file)) {
                while (l.hasNext()) {
                    triples.add(new localTriple(l.nextLine()));
                }
            } catch (IOException e) {
                e.printStackTrace();
                System.exit(-1);
            }
        } else {
            System.out.println("# Notice: " + file.getName() + " does not exist.");
        }
        return triples;
    }

    public static class localTriple {
        String head;
        long headId;
        String relation;
        String tail;
        long tailId;
        long relId = -1;

        Map<String, Object> headProperties;
        Map<String, Object> relationProperties;
        Map<String, Object> tailProperties;

        List<String> headLabels = new ArrayList<>();
        List<String> tailLabels = new ArrayList<>();

        localTriple(String h, String r, String t) {
            head = h; relation = r; tail = t;
        }

        localTriple(String line) {
            String[] words = line.split("\\s");
            head = words[0];
            relation = words[1];
            tail = words[2];
        }

        @Override
        public String toString() {
            return head + "\t" + relation + "\t" + tail;
        }

        public String toFileLine() {
            return relId + "\t" + headId + "\t" + relation + "\t" + tailId;
        }

        @Override
        public int hashCode() {
            return head.hashCode() * 5
                    + relation.hashCode() * 2
                    + tail.hashCode() * 5;
        }

        @Override
        public boolean equals(Object obj) {
            if(obj instanceof localTriple) {
                localTriple other = (localTriple) obj;
                return other.head.equals(head) && other.relation.equals(relation) && other.tail.equals(tail);
            }
            return false;
        }

        public static Set<String> getNodes(Collection<localTriple> triples) {
            Set<String> nodes = new HashSet<>();
            triples.forEach( triple -> {
                nodes.add(triple.head);
                nodes.add(triple.tail);
            });
            return nodes;
        }

        public static Set<String> getRelationshipTypes(Collection<localTriple> triples) {
            return triples.stream().map(triple -> triple.relation).collect(Collectors.toSet());
        }
    }
}
