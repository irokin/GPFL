package uk.ac.ncl.structure;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class CountedSet<T> implements Iterable<T> {
    private Map<T, Integer> map = new HashMap<>();

    @Override
    public Iterator<T> iterator() {
        return map.keySet().iterator();
    }

    public Stream<T> stream() {
        return map.keySet().stream();
    }

    public CountedSet() {}

    public CountedSet(Collection<T> base) {
        base.forEach(this::add);
    }

    public int add(T item) {
        if(!map.containsKey(item)) {
            map.put(item, 1);
        } else {
            map.put(item, map.get(item) + 1);
        }
        return map.get(item);
    }

    public void put(T item, int value) {
        map.put(item, value);
    }

    public List<T> sort(boolean desc) {
        if(desc)
            return  map.entrySet().stream().sorted(Map.Entry.comparingByValue(((o1, o2) -> o2 - o1)))
                    .map(Map.Entry::getKey).collect(Collectors.toList());
        else
            return map.entrySet().stream().sorted(Map.Entry.comparingByValue())
                    .map(Map.Entry::getKey).collect(Collectors.toList());
    }

    public int get(T item) {
        return map.getOrDefault(item, -1);
    }

    public Set<T> keySet() {
        return map.keySet();
    }

    public Set<T> topKeys(int rank) {
        return new HashSet<>(sort(true).subList(0, Math.min(keySet().size(), rank)));
    }

    public CountedSet<T> topCountedSet(int rank) {
        CountedSet<T> set = new CountedSet<>();
        sort(true).subList(0, Math.min(keySet().size(), rank)).forEach( key -> set.put(key, get(key)));
        return set;
    }

    public int size() {
        return map.size();
    }
}
