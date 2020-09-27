package uk.ac.ncl.structure;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class ScoreTree<E> {
    Set<E> values;
    List<ScoreTree<E>> children = new ArrayList<>();

    public ScoreTree() {}

    private ScoreTree(Set<E> candidates) {
        values = new HashSet<>(candidates);
    }

    public void add(Set<E> candidates)  {
        for (ScoreTree<E> child : children) {
            child.add(candidates);
        }

        if (values == null && !candidates.isEmpty()) {
            children.add(new ScoreTree<>(candidates));
        } else {
            Set<E> known = new HashSet<>();
            for (E candidate : candidates) {
                if (values.contains(candidate)) {
                    known.add(candidate);
                    values.remove(candidate);
                }
            }
            candidates.removeAll(known);

            if (!known.isEmpty())
                children.add(new ScoreTree<>(known));
        }
    }

    public List<Set<E>> getList() {
        assert values == null;
        List<Set<E>> list = new ArrayList<>();
        children.forEach(c -> c.populateList(list));
        return list;
    }

    private void populateList(List<Set<E>> list) {
        children.forEach(c -> c.populateList(list));
        if(!values.isEmpty())
            list.add(values);
    }

    public int size() {
        return getList().size();
    }
}
