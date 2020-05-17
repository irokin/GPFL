package uk.ac.ncl.validations;

import uk.ac.ncl.core.Engine;
import uk.ac.ncl.utils.IO;
import uk.ac.ncl.utils.Logger;
import com.google.common.collect.HashBasedTable;
import com.google.common.collect.Multimap;
import com.google.common.collect.MultimapBuilder;
import com.google.common.collect.Table;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.LineIterator;
import org.json.JSONArray;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.*;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.stream.Collectors;

public class Ensemble extends Engine {

    List<File> ensembleBases = new ArrayList<>();

    public Ensemble(File config, String logName) {
        super(config, logName);
        graphFile = new File(home, "databases/graph.db");
        graph = IO.loadGraph(graphFile);
        trainFile = new File(home, "data/annotated_train.txt");
        testFile = new File(home, "data/annotated_test.txt");
        validFile = new File(home, "data/annotated_valid.txt");
        JSONArray bases = args.getJSONArray("ensemble_bases");
        assert !bases.isEmpty();
        for (Object base : bases) {
            ensembleBases.add(new File(home, (String) base));
        }
    }

    public void selectBestSolutions() {
        Multimap<File, String> targetMap = MultimapBuilder.hashKeys().hashSetValues().build();
        Table<File, String, Double> table = HashBasedTable.create();
        for (File base : ensembleBases) {
            File evalLog = new File(base, "eval_log.txt");
            for (Map.Entry<String, Double> entry : readEvalLogs(evalLog).entrySet()) {
                table.put(base, entry.getKey(), entry.getValue());
            }
        }
        for (String target : table.columnKeySet()) {
            List<File> sortedFiles = table.column(target).entrySet().stream().sorted(Map.Entry.comparingByValue(((o1, o2) -> {
                double result = o2 - o1;
                if(result < 0) return -1;
                if(result > 0) return 1;
                else return 0;
            }))).map(Map.Entry::getKey).collect(Collectors.toList());
            targetMap.put(sortedFiles.get(0), target);
        }
        Logger.println("");
        for (Map.Entry<File, Collection<String>> fileCollectionEntry : targetMap.asMap().entrySet()) {
            Logger.println(fileCollectionEntry.toString());
        }
        BlockingQueue<String> contents = new LinkedBlockingDeque<>(100000);
        Thread reader = new PredictionReader(targetMap, contents);
        Thread writer = new PredictionWriter(new File(out, "predictions.txt"), contents, reader);
        try {
            reader.join();
            writer.join();
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(-1);
        }
    }

    class PredictionReader extends Thread {
        Multimap<File, String> targetMap;
        BlockingQueue<String> queue;

        PredictionReader(Multimap<File, String> targetMap, BlockingQueue<String> queue) {
            this.targetMap = targetMap;
            this.queue = queue;
            start();
        }

        @Override
        public void run() {
            for (Map.Entry<File, Collection<String>> entry : targetMap.asMap().entrySet()) {
                readPredictions(entry.getKey(), entry.getValue(), queue);
            }
        }
    }

    class PredictionWriter extends Thread {
        File in;
        BlockingQueue<String> queue;
        Thread reader;

        PredictionWriter(File in, BlockingQueue<String> queue, Thread reader) {
            this.queue = queue;
            this.in = in;
            this.reader = reader;
            start();
        }

        @Override
        public void run() {
            try(PrintWriter writer = new PrintWriter(new FileWriter(in, false))) {
                sleep(200);
                while(!queue.isEmpty() || reader.isAlive()) {
                    String content = queue.poll();
                    if(content != null) {
                        writer.println(content);
                    }
                }
            } catch (Exception e) {
                e.printStackTrace();
                System.exit(-1);
            }
        }
    }

    private void readPredictions(File file, Collection<String> targets, BlockingQueue<String> queue) {
        try(LineIterator l = FileUtils.lineIterator(new File(file, "predictions.txt"))) {
            while(l.hasNext()) {
                String line = l.nextLine();
                if(line.startsWith("Tail Query:") || line.startsWith("Head Query:")) {
                    if(targets.contains(line.split(", ")[1])) {
                        queue.put(line);
                        while (l.hasNext()) {
                            String predictionLine = l.nextLine();
                            if (predictionLine.length() != 0) {
                                queue.put(predictionLine);
                            } else {
                                queue.put("");
                                break;
                            }
                        }
                    }
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(-1);
        }
    }


    private Map<String, Double> readEvalLogs(File file) {
        Map<String, Double> results = new HashMap<>();
        try(LineIterator l = FileUtils.lineIterator(file)) {
            while(l.hasNext()) {
                String line = l.nextLine();
                if(line.split("\\s+").length == 2) {
                    String target = line.split("\\s+")[1];
                    target = target.substring(0, target.length() - 1);
                    double sum = 0d;
                    for (int i = 0; i < 5; i++) {
                        String[] words = l.nextLine().split("\\s+");
                        if(words.length == 5) {
                            sum += Double.parseDouble(words[4]);
                        }
                    }
                    results.put(target, sum);
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
            System.exit(-1);
        }
        assert !results.isEmpty();
        return results;
    }
}
