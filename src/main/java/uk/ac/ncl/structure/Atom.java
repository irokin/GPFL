package uk.ac.ncl.structure;

import uk.ac.ncl.Settings;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;

/**
 * A rule is composed of head and body atoms. Each atom has predicate and terms.
 * A term can be a variable or a constant.
 */
public class Atom {
    public RelationshipType type;
    public Direction direction;
    public String predicate;
    public String subject;
    public long subjectId;
    public String object;
    public long objectId;

    public Atom(String line, boolean head) {
        predicate = line.split("\\(")[0];
        String[] words = line.split("\\(")[1].split(",");
        String start = words[0];
        String end = words[1].replace(")", "");
        if(head) {
            direction = Direction.OUTGOING;
            subject = start;
            object = end;
        } else {
            direction = words[2].equals("0)") ? Direction.OUTGOING : Direction.INCOMING;
            if(direction.equals(Direction.OUTGOING)) {
                subject = start;
                object = end;
            } else {
                subject = end;
                object = start;
            }
        }
    }

    public Atom(Atom base) {
        predicate = base.predicate;
        subject = base.subject;
        subjectId = base.subjectId;
        objectId = base.objectId;
        object = base.object;
        direction = base.direction;
        type = base.type;
    }

    /**
     * Init head atom with info provided by instance.
     */
    public Atom(Pair pair) {
        type = pair.type;
        predicate = pair.type.name();
        subject = pair.subName;
        subjectId = pair.subId;
        object = pair.objName;
        objectId = pair.objId;
        direction = Direction.OUTGOING;
    }

    /**
     * This Atom structure always ensures that the subject
     */
    public Atom(Node source, Relationship relationship) {
        boolean inverse = source.equals(relationship.getEndNode());
        type = relationship.getType();
        predicate = relationship.getType().name();
        if ( inverse ) {
            direction = Direction.INCOMING;
            subject = (String) relationship.getEndNode().getProperty(Settings.NEO4J_IDENTIFIER);
            object = (String) relationship.getStartNode().getProperty(Settings.NEO4J_IDENTIFIER);
            subjectId = relationship.getEndNodeId();
            objectId = relationship.getStartNodeId();
        }
        else  {
            direction = Direction.OUTGOING;
            subject = (String) relationship.getStartNode().getProperty(Settings.NEO4J_IDENTIFIER);
            object = (String) relationship.getEndNode().getProperty(Settings.NEO4J_IDENTIFIER);
            subjectId = relationship.getStartNodeId();
            objectId = relationship.getEndNodeId();
        }
    }

    public String getSubject() {
        return subject;
    }

    public void setSubject(String subject) {
        this.subject = subject;
    }

    public String getObject() {
        return object;
    }

    public void setObject(String object) {
        this.object = object;
    }

    public long getSubjectId() {
        return subjectId;
    }

    public void setSubjectId(long subjectId) {
        this.subjectId = subjectId;
    }

    public long getObjectId() {
        return objectId;
    }

    public void setObjectId(long objectId) {
        this.objectId = objectId;
    }

    public boolean isInverse() {
        return direction.equals(Direction.INCOMING);
    }

    public String getBasePredicate() {
        return predicate;
    }

    @Override
    public String toString() {
        if(direction.equals(Direction.INCOMING))
            return predicate + "(" + object + "," + subject +")";
        else
            return predicate + "(" + subject + "," + object + ")";
    }

    public String toRuleIndexString() {
        if(direction.equals(Direction.INCOMING))
            return predicate + "(" + object + "," + subject + "," + 1 + ")";
        else
            return predicate + "(" + subject + "," + object + "," + 0 + ")";
    }

    @Override
    public int hashCode() {
        return this.toString().hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        if ( obj instanceof Atom) {
            Atom right = (Atom) obj;
            return this.toString().equals(right.toString());
        }
        return false;
    }

}
