package uk.ac.ncl.structure;

import java.util.Set;

public class Package {
    static int globalIndex = 0;
    public static void resetIndex() {
        globalIndex = 0;
    }
    public static Package create(Rule r) {
        return new Package(r, globalIndex++);
    }

    public Set<Pair> candidates;
    public Rule rule;
    public int id;

    private Package(Rule rule, int id) {
        this.rule = rule;
        this.id = id;
    }
}