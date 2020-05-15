package ac.uk.ncl.structure;

import ac.uk.ncl.Settings;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;

import java.text.MessageFormat;

public class Instance {
    final public RelationshipType type;
    final public long startNodeId;
    final public long endNodeId;
    final public String startNodeName;
    final public String endNodeName;
    final public Relationship relationship;

    public Instance(Relationship relationship) {
        this.relationship = relationship;
        type = relationship.getType();
        startNodeId = relationship.getStartNodeId();
        endNodeId = relationship.getEndNodeId();
        startNodeName = (String) relationship.getStartNode().getProperty(Settings.NEO4J_IDENTIFIER);
        endNodeName = (String) relationship.getEndNode().getProperty(Settings.NEO4J_IDENTIFIER);
    }

    public Pair toPair() {
        return new Pair(startNodeId, endNodeId);
    }

    @Override
    public int hashCode() {
        return   (int) startNodeId * 12 + (int) endNodeId * 13
                + startNodeName.hashCode() + endNodeName.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        if(obj instanceof Instance) {
            Instance right = (Instance) obj;
            return startNodeId == right.startNodeId && endNodeId == right.endNodeId;
        }
        return false;
    }

    @Override
    public String toString() {
        return MessageFormat.format("[{0},{1}]", startNodeName, endNodeName);
    }
}
