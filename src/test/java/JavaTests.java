import com.google.common.collect.Sets;
import org.junit.Test;

import java.util.*;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingDeque;

public class JavaTests {

    @Test
    public void treeTests() {
        ScoreTree<String> root = new ScoreTree<>();
        root.add(Sets.newHashSet("a", "b", "c", "d", "e", "f"));
        root.add(Sets.newHashSet("a", "b", "c"));
        root.add(Sets.newHashSet("a", "b"));
        root.add(Sets.newHashSet("f"));
        root.add(Sets.newHashSet("c"));
        root.add(Sets.newHashSet("e"));
        root.add(Sets.newHashSet("g"));
        root.asGroups().forEach(System.out::println);
        System.out.println(root.size());
    }
    
    static class ScoreTree<E> {
        Set<E> values;
        double score;
        List<ScoreTree<E>> children = new ArrayList<>();

        public ScoreTree() {}

        private ScoreTree(Set<E> candidates) {
            values = new HashSet<>(candidates);
            this.score = score;
        }

        public void add(Set<E> candidates)  {
            for (ScoreTree<E> child : children) {
                child.add(candidates);
            }

            if (values == null) {
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
                if (!known.isEmpty()) children.add(new ScoreTree<>(known));
            }
        }

        public List<Set<E>> asGroups() {
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
            return asGroups().size();
        }
    }

    @Test
    public void orderedQueue() {
        BlockingQueue<Package> input = new LinkedBlockingDeque<>();
        for (String s : Arrays.asList("a", "b", "c", "d", "e", "f", "g", "h", "i", "a", "b", "c", "d", "e", "f", "g", "h", "i", "a", "b", "c", "d", "e", "f", "g", "h", "i", "a", "b", "c", "d", "e", "f", "g", "h", "i","a", "b", "c", "d", "e", "f", "g", "h", "i", "a", "b", "c", "d", "e", "f", "g", "h", "i")) {
            input.add(Package.create(s));
        }

        ConcurrentHashMap<Integer, String> queue = new ConcurrentHashMap<>();
        Dispatcher dispatcher = new Dispatcher(queue, input);
        Producer[] producers = new Producer[4];
        for (int i = 0; i < producers.length; i++) {
            producers[i] = new Producer(i, dispatcher, queue, input);
        }
        try {
            for (Producer producer : producers) {
                producer.join();
            }
        } catch (InterruptedException e) {
            e.printStackTrace();
            System.exit(-1);
        }
    }

    static class Producer extends Thread {
        Dispatcher dispatcher;
        ConcurrentHashMap<Integer, String> queue;
        BlockingQueue<Package> input;

        public Producer(int id, Dispatcher dispatcher, ConcurrentHashMap<Integer, String> queue, BlockingQueue<Package> input) {
            super("producer-" + id);
            this.dispatcher = dispatcher;
            this.queue = queue;
            this.input = input;
            start();
        }

        @Override
        public void run()  {
            while(dispatcher.isAlive()) {
                if (queue.size() < 100) {
                    Package p = input.poll();
                    if (p != null)
                        queue.put(p.index, p.item);
                }
            }
        }
    }

    static class Dispatcher extends Thread {
        ConcurrentHashMap<Integer, String> queue;
        BlockingQueue<Package> input;
        int current = 0;

        public Dispatcher(ConcurrentHashMap<Integer, String>queue, BlockingQueue<Package> input) {
            this.queue = queue;
            this.input = input;
            start();
        }

        @Override
        public void run() {
            while(!input.isEmpty() || !queue.isEmpty()) {
                if(queue.containsKey(current)) {
                    System.out.println(queue.remove(current++));
                }
            }
        }
    }

    static class Package {
        static int globalIndex = 0;
        public static Package create(String item) {
            return new Package(globalIndex++, item);
        }

        int index;
        String item;
        private Package(int id, String item) {
            this.index = id;
            this.item = item;
        }
    }

    @Test
    public void queueTest() {
        ScoreList<String> root = new ScoreList<>();
        root.add(Sets.newHashSet("a", "b", "c", "d", "e", "f"));
        root.add(Sets.newHashSet("a", "b", "c"));
        root.add(Sets.newHashSet("a", "b"));
        root.add(Sets.newHashSet("f"));
        root.add(Sets.newHashSet("c"));
        root.add(Sets.newHashSet("e"));
        root.add(Sets.newHashSet("g"));

        root.list.forEach(System.out::println);
    }

    static class ScoreList<E> {
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
    }


}
