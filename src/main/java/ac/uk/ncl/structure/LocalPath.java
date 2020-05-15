package ac.uk.ncl.structure;

import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;

import java.util.ArrayList;
import java.util.List;

public class LocalPath {
    public List<Relationship> relationships = new ArrayList<>();
    public List<Direction> directions = new ArrayList<>();
    public List<Node> nodes = new ArrayList<>();

    Relationship lastRelationship;

    public LocalPath(LocalPath base, Relationship added) {
        relationships.addAll(base.relationships);
        nodes.addAll(base.nodes);
        directions.addAll(base.directions);

        Node endNode = getEndNode();
        if(added.getStartNode().equals(endNode)) directions.add(Direction.OUTGOING);
        else directions.add(Direction.INCOMING);

        relationships.add(added);
        nodes.add(added.getOtherNode(endNode));

        lastRelationship = added;
    }

    public LocalPath(Relationship l, Direction d) {
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

    public LocalPath(Node node, Relationship l) {
        nodes.add(node);
        lastRelationship = l;
    }

    public int length() {
        return relationships.size();
    }

    public Node getEndNode() {
        return nodes.get(nodes.size() - 1);
    }

    public Node getStartNode() {
        return nodes.get(0);
    }

    @Override
    public String toString() {
        return relationships.toString();
    }
}
