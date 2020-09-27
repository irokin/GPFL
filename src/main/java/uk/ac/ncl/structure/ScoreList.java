package uk.ac.ncl.structure;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class ScoreList<E> {
    List<Set<E>> list = new ArrayList<>();

    public ScoreList() {}

    public void add(Set<E> candidates) {
        List<Set<E>> temp = new ArrayList<>(list);
        Set<E> unassigned = new HashSet<>(candidates);
        for (Set<E> e : list) {
            Set<E> backwardBuffer = new HashSet<>();
            for (E candidate : candidates) {
                if(e.contains(candidate)) {
                    backwardBuffer.add(candidate);
                    unassigned.remove(candidate);
                }
            }
            if(!backwardBuffer.isEmpty() && backwardBuffer.size() != e.size()) {
                e.removeAll(backwardBuffer);
                temp.add(temp.indexOf(e), backwardBuffer);
            }
        }
        if(!unassigned.isEmpty())
            temp.add(unassigned);
        list = temp;
    }

    public List<Set<E>> getList() {
        return list;
    }
}
