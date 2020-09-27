package uk.ac.ncl.graph;

public class InMemoryRel {
    final private InMemoryNode startNode;
    final private InMemoryNode endNode;
    final private String type;

    public InMemoryRel(InMemoryNode startNode, InMemoryNode endNode, String type) {
        this.startNode = startNode;
        this.endNode = endNode;
        this.type = type;
    }

    public InMemoryNode getStartNode() {
        return startNode;
    }

    public InMemoryNode getEndNode() {
        return endNode;
    }

    public String getType() {
        return type;
    }

    public InMemoryNode getOtherNode(InMemoryNode node) {
        if(startNode.equals(node)) return endNode;
        else if(endNode.equals(node)) return startNode;
        else return null;
    }

    @Override
    public int hashCode() {
        return startNode.hashCode() + endNode.hashCode() + type.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        if(obj instanceof InMemoryRel) {
            InMemoryRel other = (InMemoryRel) obj;
            return other.startNode.equals(this.startNode) &&
                    other.endNode.equals(this.endNode) &&
                    other.type.equals(this.type);
        }
        return false;
    }

    @Override
    public String toString() {
        return startNode + "-" + type + "->" + endNode;
    }
}
