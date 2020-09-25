package uk.ac.ncl.structure;

import com.google.common.collect.BiMap;
import uk.ac.ncl.Settings;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Path;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;

import java.text.MessageFormat;
import java.util.Comparator;

public class Pair {
    public long subId, objId, relId; // Ids
    public Double[] scores;
    public String subName, objName, typeName; //Names
    public Relationship rel;
    public RelationshipType type;

    public Pair(long subId, long objId, long relId, Relationship rel, RelationshipType type
            , String subName, String objName, String typeName) {
        this.subId = subId;
        this.objId = objId;
        this.relId = relId;
        this.subName = subName;
        this.objName = objName;
        this.typeName = typeName;
        this.rel = rel;
        this.type = type;
    }

    public Pair(long sub, long obj) {
        this.subId = sub;
        this.objId = obj;
    }

    public Pair(long sub, long obj, long relId) {
        this(sub, obj);
        this.relId = relId;
    }

    public Pair(long subId, String subName
            , long objId, String objName
            , long relId, String relName) {
        this.subId = subId;
        this.subName = subName;
        this.objId = objId;
        this.objName = objName;
        this.relId = relId;
        this.typeName = relName;
    }

    public Pair(Path path) {
        this.subId = path.startNode().getId();
        this.objId = path.endNode().getId();
    }

    public Pair(String subName, String objName) {
        this.subName = subName;
        this.objName = objName;
    }

    public Pair(String subName, String relName, String objName) {
        this(subName, objName);
        typeName = relName;
    }

    public Pair(String[] words, BiMap<String, Long> nodeIndex) {
        this.subName = words[0];
        this.subId = nodeIndex.get(subName);
        this.objName = words[2];
        this.objId = nodeIndex.get(objName);
        this.type = RelationshipType.withName(words[1]);
    }

    public boolean isSelfloop() {
        return subId == objId;
    }

    @Override
    public int hashCode() {
        return (int) (subId * 20 + objId * 31);
    }

    @Override
    public boolean equals(Object obj) {
        if ( obj instanceof Pair) {
            Pair right = (Pair) obj;
            return this.subId == right.subId && this.objId == right.objId;
        } return false;
    }

    @Override
    public String toString() {
        return MessageFormat.format("[{0},{1}]", String.valueOf(subId), String.valueOf(objId));
    }

    public static Comparator<Pair> scoresComparator(int l) {
        return (o1, o2) -> {
            if(o1.scores.length > l && o2.scores.length > l) {
                double result = o2.scores[l] - o1.scores[l];
                if (result > 0) return 1;
                else if (result < 0) return -1;
                else return 0;
            } else {
                int result = o2.scores.length - o1.scores.length;
                if(result > 0) return 1;
                else if (result < 0) return -1;
                else return 0;
            }
        };
    }

    public String toQueryString(GraphDatabaseService graph) {
        return "(" + subId + "|" + graph.getNodeById(subId).getProperty(Settings.NEO4J_IDENTIFIER)
                + ", "  + Settings.TARGET + ", "
                + objId + "|" + graph.getNodeById(objId).getProperty(Settings.NEO4J_IDENTIFIER) + ")";
    }

    public String toVerificationString(GraphDatabaseService graph) {
        return graph.getNodeById(subId).getProperty(Settings.NEO4J_IDENTIFIER)
                + "\t"  + Settings.TARGET + "\t"
                + graph.getNodeById(objId).getProperty(Settings.NEO4J_IDENTIFIER);
    }

    public String toTripleString() {
        return String.join("\t", new String[]{subName, typeName, objName});
    }

    public String toAnnotatedString() {
        return String.join("\t", new String[]{String.valueOf(relId), String.valueOf(subId), typeName, String.valueOf(objId)});
    }

    public boolean insEqual(Pair other) {
        return subName.equals(other.subName)
                && typeName.equals(other.typeName)
                && objName.equals(other.objName);
    }
}
