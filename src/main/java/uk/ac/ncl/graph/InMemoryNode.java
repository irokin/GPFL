package uk.ac.ncl.graph;

import org.neo4j.graphdb.Node;

public class InMemoryNode {
    private long id;
    private String name;

    public InMemoryNode(long id, String name) {
        this.id = id;
        this.name = name;
    }

    public long getId() {
        return id;
    }

    public String getName() {
        return name;
    }

    @Override
    public int hashCode() {
        return name.hashCode() + (int) id * 4;
    }

    @Override
    public boolean equals(Object obj) {
        if(obj instanceof InMemoryNode) {
            InMemoryNode other = (InMemoryNode) obj;
            return other.name.equals(this.name) && other.id == this.id;
        }
        return false;
    }

    @Override
    public String toString() {
        return name;
    }
}
