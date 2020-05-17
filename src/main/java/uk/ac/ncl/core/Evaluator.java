package uk.ac.ncl.core;

import uk.ac.ncl.Settings;
import uk.ac.ncl.structure.Pair;
import uk.ac.ncl.structure.Rule;
import uk.ac.ncl.structure.Triple;
import uk.ac.ncl.utils.Helpers;
import uk.ac.ncl.utils.IO;
import uk.ac.ncl.utils.Logger;
import com.google.common.collect.Lists;
import com.google.common.collect.Multimap;
import com.google.common.collect.MultimapBuilder;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.LineIterator;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Transaction;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.text.DecimalFormat;
import java.util.*;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingDeque;

/**
 * A new AnyBURL style evaluator replacing the original one.
 */
public class Evaluator {
    final private BlockingQueue<Pair> testPairs;
    Multimap<Long, Pair> subIndex = MultimapBuilder.hashKeys().hashSetValues().build();
    Multimap<Long, Pair> objIndex = MultimapBuilder.hashKeys().hashSetValues().build();

    GraphDatabaseService graph;
    BlockingQueue<String> predictionContentQueue = new LinkedBlockingDeque<>(100000);
    BlockingQueue<String> verificationContentQueue = new LinkedBlockingDeque<>(100000);
    File predictionFile;
    File verificationFile;
    Multimap<Pair, Rule> candidates;
    Set<Pair> filterSet;

    static DecimalFormat f = new DecimalFormat("####.#####");
    static Multimap<String, Integer> rankMap;
    static Multimap<String, Integer> headMap;
    static Multimap<String, Integer> tailMap;

    public Evaluator(Set<Pair> testPairs
            , Set<Pair> filterSet
            , Context context
            , File predictionFile
            , File verificationFile
            , GraphDatabaseService graph) {
        this.testPairs = new LinkedBlockingDeque<>(testPairs);
        this.predictionFile = predictionFile;
        this.verificationFile = verificationFile;
        this.candidates = context.getPredictionMultiMap();
        this.graph = graph;
        this.filterSet = filterSet;

        for (Pair pair : candidates.keySet()) {
            subIndex.put(pair.subId, pair);
            objIndex.put(pair.objId, pair);
        }
    }

    public void createQueries() {
        long s = System.currentTimeMillis();
        Thread[] queryCreators = new QueryCreator[Settings.THREAD_NUMBER];
        for (int i = 0; i < queryCreators.length; i++) {
            queryCreators[i] = new QueryCreator(i);
        }
        WriterTask predictionWriter = new WriterTask(queryCreators, predictionFile, predictionContentQueue);
        WriterTask verificationWriter = new WriterTask(queryCreators, verificationFile, verificationContentQueue);
        try {
            for (Thread thread : queryCreators) {
                thread.join();
            }
            predictionWriter.join();
            verificationWriter.join();
        } catch (InterruptedException e) {
            e.printStackTrace();
            System.exit(-1);
        }
        Helpers.timerAndMemory(s, "# Create Queries");
    }

    static public void evalAnyBURL(String home) {
        Logger.init(new File(home, "eval_log.txt"), false);
        Multimap<String, Triple> filterMap = buildFilterMap(home);
        scoreAnyBURL(filterMap, new File(home, "predictions.txt"));
    }

    static public Multimap<String, Triple> buildFilterMap(String home) {
        Multimap<String, Triple> filterMap = MultimapBuilder.hashKeys().arrayListValues().build();
        if(Settings.POST_FILTERING) {
            readTriples(new File(home, "data/train.txt")).forEach(triple -> filterMap.put(triple.pred, triple));
            readTriples(new File(home, "data/test.txt")).forEach(triple -> filterMap.put(triple.pred, triple));
            readTriples(new File(home, "data/valid.txt")).forEach(triple -> filterMap.put(triple.pred, triple));
        }
        return filterMap;
    }

    static public Set<Triple> readTriples(File f) {
        Set<Triple> triples = new HashSet<>();
        try(LineIterator l = FileUtils.lineIterator(f)) {
            while(l.hasNext()) {
                triples.add(new Triple(l.nextLine(), 0));
            }
        } catch (IOException e) {
            e.printStackTrace();
            System.exit(-1);
        }
        return triples;
    }

    static private void scoreAnyBURL(Multimap<String, Triple> filterMap, File file) {
        rankMap = MultimapBuilder.hashKeys().arrayListValues().build();
        headMap = MultimapBuilder.hashKeys().arrayListValues().build();
        tailMap = MultimapBuilder.hashKeys().arrayListValues().build();

        try(LineIterator l = FileUtils.lineIterator(file)) {
            while(l.hasNext()) {
                Triple testTriple = new Triple(l.nextLine(), 0);
                int headRank = readAnyBURLRank(filterMap.get(testTriple.pred), testTriple, l.nextLine());
                int tailRank = readAnyBURLRank(filterMap.get(testTriple.pred), testTriple, l.nextLine());
                rankMap.put(testTriple.pred, headRank);
                rankMap.put(testTriple.pred, tailRank);
                headMap.put(testTriple.pred, headRank);
                tailMap.put(testTriple.pred, tailRank);
            }
        } catch (IOException e) {
            e.printStackTrace();
            System.exit(-1);
        }

        reportResults();
    }

    static private void reportResults() {
        Multimap<String, Double> perPredicateResults = MultimapBuilder.hashKeys().arrayListValues().build();
        for (String predicate : rankMap.keySet()) {
            printResultsSinglePredicate(predicate, new ArrayList<>(headMap.get(predicate))
                    , new ArrayList<>(tailMap.get(predicate))
                    , new ArrayList<>(rankMap.get(predicate))
                    , 3, perPredicateResults, true);
        }
        if(Settings.EVAL_PROTOCOL.equals("TransE")) {
            printResultsSinglePredicate("TransE Protocol - All Targets", new ArrayList<>(headMap.values())
                    , new ArrayList<>(tailMap.values())
                    , new ArrayList<>(rankMap.values())
                    , 1, perPredicateResults, false);
        } else if(Settings.EVAL_PROTOCOL.equals("GPFL")) {
            printAverageResults(perPredicateResults);
        } else if(Settings.EVAL_PROTOCOL.equals("All")) {
            printResultsSinglePredicate("TransE Protocol - All Targets", new ArrayList<>(headMap.values())
                    , new ArrayList<>(tailMap.values())
                    , new ArrayList<>(rankMap.values())
                    , 1, perPredicateResults, false);
            printAverageResults(perPredicateResults);
        } else {
            System.err.println("# Unknown Evaluation Protocol is selected.");
            System.exit(-1);
        }
    }

    static private void printAverageResults(Multimap<String, Double> perPredicateResults) {
        DecimalFormat f = new DecimalFormat("###.####");
        f.setMinimumFractionDigits(4);
        StringBuilder sb = new StringBuilder("# " + "GPFL Protocol - All Targets:\n"
                + "#           Head     Tail     All\n");
        double[] scores = new double[12];
        for (String predicate : perPredicateResults.keySet()) {
            List<Double> predicateScores = new ArrayList<>(perPredicateResults.get(predicate));
            assert predicateScores.size() == 12;
            for (int i = 0; i < predicateScores.size(); i++) {
                scores[i] += predicateScores.get(i);
            }
        }
        for (int i = 0; i < scores.length; i++) {
            scores[i] = scores[i] / perPredicateResults.keySet().size();
        }
        int[] ns = new int[]{1, 3};
        int index = 0;
        for (int n : ns) {
            sb.append("# hits@" + n + ":   ");
            sb.append((f.format(scores[index++]) + "   "));
            sb.append((f.format(scores[index++]) + "   "));
            sb.append((f.format(scores[index++]) + "\n"));
        }
        sb.append("# hits@10:  ");
        sb.append((f.format(scores[index++]) + "   "));
        sb.append((f.format(scores[index++]) + "   "));
        sb.append((f.format(scores[index++]) + "\n"));

        sb.append("# MRR:      ");
        sb.append((f.format(scores[index++]) + "   "));
        sb.append((f.format(scores[index++]) + "   "));
        sb.append((f.format(scores[index++]) + "\n"));
        Logger.println(sb.toString(), 1);
    }

    static private void printResultsSinglePredicate(String predicate, List<Integer> headRanks, List<Integer> tailRanks
            , List<Integer> allRanks, int verb, Multimap<String, Double> perPredicateResults, boolean add) {
        DecimalFormat f = new DecimalFormat("###.####");
        double headScore, tailScore, allScore;
        f.setMinimumFractionDigits(4);
        int[] ns = new int[]{1,3};
        StringBuilder sb = new StringBuilder("# " + predicate + ":\n#           Head     Tail     All\n");
        for (int n : ns) {
            headScore = hitsAt(headRanks, n);
            if(add) perPredicateResults.put(predicate, headScore);
            tailScore = hitsAt(tailRanks, n);
            if(add) perPredicateResults.put(predicate, tailScore);
            allScore = hitsAt(allRanks, n);
            if(add) perPredicateResults.put(predicate, allScore);

            sb.append("# hits@" + n + ":   ");
            sb.append((headRanks.isEmpty() ? "No Records\t" : f.format(headScore) + "   "));
            sb.append((tailRanks.isEmpty() ? "No Records\t" : f.format(tailScore) + "   "));
            sb.append((allRanks.isEmpty() ? "No Records\n" : f.format(allScore) + "\n"));

        }
        headScore = hitsAt(headRanks, 10);
        if(add) perPredicateResults.put(predicate, headScore);
        tailScore = hitsAt(tailRanks, 10);
        if(add) perPredicateResults.put(predicate, tailScore);
        allScore = hitsAt(allRanks, 10);
        if(add) perPredicateResults.put(predicate, allScore);
        sb.append("# hits@10:  ");
        sb.append((headRanks.isEmpty() ? "No Records\t" : f.format(headScore) + "   "));
        sb.append((tailRanks.isEmpty() ? "No Records\t" : f.format(tailScore) + "   "));
        sb.append((allRanks.isEmpty() ? "No Records\n" : f.format(allScore) + "\n"));

        headScore = mrr(headRanks);
        if(add) perPredicateResults.put(predicate, headScore);
        tailScore = mrr(tailRanks);
        if(add) perPredicateResults.put(predicate, tailScore);
        allScore = mrr(allRanks);
        if(add) perPredicateResults.put(predicate, allScore);
        sb.append("# MRR:      ");
        sb.append((headRanks.isEmpty() ? "No Records\t" : f.format(headScore) + "   "));
        sb.append((tailRanks.isEmpty() ? "No Records\t" : f.format(tailScore) + "   "));
        sb.append((allRanks.isEmpty() ? "No Records\n" : f.format(allScore) + "\n"));
        Logger.println(sb.toString(), verb);
    }

    static private int readAnyBURLRank(Collection<Triple> filterSet, Triple testTriple, String line) {
        boolean headQuery = line.startsWith("Heads: ");
        String[] splits = headQuery ? line.split("Heads: ") : line.split("Tails: ");
        if(splits.length <= 1) return 0;
        String[] words = splits[1].split("\t");

        List<Triple> currentAnswers = new ArrayList<>();
        for (int i = 0; i < words.length; i++) {
            if(i % 2 == 0) {
                if(headQuery)
                    currentAnswers.add(new Triple(words[i], testTriple.pred, testTriple.obj));
                else
                    currentAnswers.add(new Triple(testTriple.sub, testTriple.pred, words[i]));
            }
        }
        int filterCount = 0;
        int rank = 0;
        for (Triple currentAnswer : currentAnswers) {
            if(filterSet.contains(currentAnswer) && !currentAnswer.equals(testTriple))
                filterCount++;
            if(currentAnswer.equals(testTriple)) {
                rank = currentAnswers.indexOf(currentAnswer);
                return rank - filterCount + 1;
            }
        }
        return rank;
    }

    static public void evalGPFL(String home) {
        Logger.init(new File(home, "eval_log.txt"), false);
        Multimap<String, Triple> filterMap = Evaluator.buildFilterMap(home);
        scoreGPFL(filterMap, new File(home, "predictions.txt"));
    }

    static public void scoreGPFL(Multimap<String, Triple> filterMap, File predictionFile) {
        rankMap = MultimapBuilder.hashKeys().arrayListValues().build();
        headMap = MultimapBuilder.hashKeys().arrayListValues().build();
        tailMap = MultimapBuilder.hashKeys().arrayListValues().build();

        try(LineIterator l = FileUtils.lineIterator(predictionFile)) {
            while(l.hasNext()) {
                String line = l.nextLine();
                boolean headQuery = line.startsWith("Head Query: ");
                Triple testTriple = headQuery ? new Triple(line.split("Head Query: ")[1], 1) :
                        new Triple(line.split("Tail Query: ")[1], 1);
                List<Triple> currentAnswers = new ArrayList<>();
                while(l.hasNext()) {
                    String predictionLine = l.nextLine();
                    if(!predictionLine.equals("")) {
                        currentAnswers.add(new Triple(predictionLine.split("\t")[0], 1));
                    }
                    else break;
                }
                int rank = 0;
                int filterCount = 0;
                Collection<Triple> filterSet = filterMap.get(testTriple.pred);
                for (Triple currentAnswer : currentAnswers) {
                    if(currentAnswer.equals(testTriple)) {
                        rank = currentAnswers.indexOf(currentAnswer) - filterCount + 1;
                        break;
                    }
                    if(filterSet.contains(currentAnswer))
                        filterCount++;
                }
                rankMap.put(testTriple.pred, rank);
                if(headQuery)
                    headMap.put(testTriple.pred, rank);
                else
                    tailMap.put(testTriple.pred, rank);
            }
        } catch (IOException e) {
            e.printStackTrace();
            System.exit(-1);
        }

        reportResults();
    }

    static private double hitsAt(List<Integer> ranks, int n) {
        int sum = 0;
        for (Integer rank : ranks) {
            if(rank != 0 && rank <= n) sum++;
        }
        return sum == 0 ? 0 : (double) sum / ranks.size();
    }

    static private double mrr(List<Integer> ranks) {
        double sum = 0;
        for (Integer rank : ranks) {
            sum += rank == 0 ? 0 : (double) 1 / rank;
        }
        return sum == 0 ? 0 : sum / ranks.size();
    }

    class QueryCreator extends Thread {
        int id;

        QueryCreator(int id) {
            super("QueryCreator-" + id);
            this.id = id;
            start();
        }

        @Override
        public void run() {
            try(Transaction tx = graph.beginTx()) {
                while (!testPairs.isEmpty()) {
                    Pair testPair = testPairs.poll();
                    if (testPair != null) {
                        Collection<Pair> tailAnswers = Settings.PRIOR_FILTERING ?
                                filter(subIndex.get(testPair.subId), testPair) : subIndex.get(testPair.subId);
                        predictionContentQueue.put(createQueryAnswers("Tail Query: ", testPair, tailAnswers));
                        Collection<Pair> headAnswers = Settings.PRIOR_FILTERING ?
                                filter(objIndex.get(testPair.objId), testPair) : objIndex.get(testPair.objId);
                        predictionContentQueue.put(createQueryAnswers("Head Query: ", testPair, headAnswers));
                    }
                }
                tx.success();
            } catch (InterruptedException e) {
                e.printStackTrace();
                System.exit(-1);
            }
        }

        private String createQueryAnswers(String header, Pair testPair, Collection<Pair> answers) {
            String content = header + testPair.toQueryString(graph) + "\n";
            List<Pair> rankedAnswers = new ArrayList<>();
            if(!answers.isEmpty()) {
                rankedAnswers = rankCandidates(answers, candidates);
                for (Pair answer : rankedAnswers.subList(0, Math.min(rankedAnswers.size(), Settings.TOP_K))) {
                    content += answer.toQueryString(graph) + "\t" + f.format(answer.scores[0]) + "\n";
                }
            }
            populateVerification(header, testPair, rankedAnswers);
            content += "\n";
            return content;
        }

        private void populateVerification(String header, Pair testPair, List<Pair> rankedAnswers) {
            int topAnswers = Settings.VERIFY_PREDICTION_SIZE;
            int topRules = Settings.VERIFY_RULE_SIZE;

            String verificationContent = header + testPair.toVerificationString(graph) + "\n";
            if(rankedAnswers.isEmpty()) {
                verificationContent += "\n";
                verificationContentQueue.add(verificationContent);
                return;
            }

            int count = 1;
            for (Pair pair : rankedAnswers.subList(0, Math.min(topAnswers, rankedAnswers.size()))) {
                verificationContent += "Top Answer: " + count + "\t" + pair.toVerificationString(graph) + "\n";
                List<Rule> rules = new ArrayList<>(candidates.get(pair));
                rules.sort(IO.ruleComparatorBySC());
                for (Rule rule : rules.subList(0, Math.min(topRules, rules.size()))) {
                    verificationContent += rule + "\t" + f.format(rule.getQuality()) + "\n";
                }
                verificationContent += "\n";
                count++;
            }

            if(rankedAnswers.contains(testPair)) {
                verificationContent += "Correct Answer: " + (rankedAnswers.indexOf(testPair) + 1) +  "\t" + testPair.toVerificationString(graph) + "\n";
                List<Rule> rules = new ArrayList<>(candidates.get(testPair));
                rules.sort(IO.ruleComparatorBySC());
                for (Rule rule : rules.subList(0, Math.min(topRules, rules.size()))) {
                    verificationContent += rule + "\t" + f.format(rule.getQuality()) + "\n";
                }
                verificationContent += "\n";
            } else {
                verificationContent += "No Correct Answer\n\n";
            }

            verificationContentQueue.add(verificationContent);
        }

        private Set<Pair> filter(Collection<Pair> answers, Pair testPair) {
            Set<Pair> filtered = new HashSet<>();
            for (Pair answer : answers) {
                if(!filterSet.contains(answer) || answer.equals(testPair))
                    filtered.add(answer);
            }
            return filtered;
        }
    }

    class WriterTask extends Thread {
        Thread[] threads;
        File file;
        BlockingQueue<String> contentQueue;

        public WriterTask(Thread[] threads, File file, BlockingQueue<String> contentQueue) {
            this.threads = threads;
            this.file = file;
            this.contentQueue = contentQueue;
            start();
        }

        @Override
        public void run() {
            boolean stop = false;
            try(PrintWriter writer = new PrintWriter(new FileWriter(file, true))) {
                while(!stop || !contentQueue.isEmpty()) {
                    String line = contentQueue.poll();
                    if(line != null) writer.print(line);
                    boolean flag = true;
                    for (Thread producer : threads) {
                        if(producer.isAlive()) flag = false;
                    }
                    if(flag) stop = true;
                }
            } catch (IOException e) {
                e.printStackTrace();
                System.exit(-1);
            }
        }
    }

    protected List<Pair> rankCandidates(Collection<Pair> answers, Multimap<Pair, Rule> ruleMap) {
        for (Pair pair : answers) {
            Double[] scores = new Double[ruleMap.get(pair).size()];
            int count = 0;
            for (Rule rule : ruleMap.get(pair)) scores[count++] = rule.getQuality();
            Arrays.sort(scores, Comparator.reverseOrder());
            pair.scores = scores;
        }
        return sortTies(answers.toArray(new Pair[0]), 0);
    }

    protected List<Pair> sortTies(Pair[] ar, int l) {
        Arrays.sort(ar, Pair.scoresComparator(l));
        List<Pair> set = Lists.newArrayList();
        Multimap<Pair, Pair> ties = MultimapBuilder.hashKeys().hashSetValues().build();
        createTies(ar, set, ties, l);

        //A base case to avoid deep recursion introducing stack overflow
        if(l > Settings.MAX_RECURSION_DEPTH) return Arrays.asList(ar);

        List<Pair> sorted = Lists.newArrayList();
        for(Pair i : set) {
            if(ties.containsKey(i)) {
                Pair[] ar1 = ties.get(i).toArray(new Pair[0]);
                sorted.addAll(sortTies(ar1, l + 1));
            } else sorted.add(i);
        }
        return sorted;
    }

    protected void createTies(Pair[] ar, List<Pair> set, Multimap<Pair, Pair> ties, int l) {
        for(int i = 0; i < ar.length; i++) {
            final int here = i;
            if(i == ar.length - 1) set.add(ar[here]);
            for(int j = i + 1; j < ar.length; j++) {
                double p = -1, q = -1;
                if(l < ar[here].scores.length) p = ar[here].scores[l];
                if(l < ar[j].scores.length) q = ar[j].scores[l];
                if(p == q && p != -1) {
                    i++;
                    if(!ties.keySet().contains(ar[here])) ties.put(ar[here], ar[here]);
                    if(i == ar.length - 1) set.add(ar[here]);
                    ties.put(ar[here], ar[j]);
                } else {
                    set.add(ar[here]);
                    break;
                }
            }
        }
    }

}
