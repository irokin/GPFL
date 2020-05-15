package ac.uk.ncl.analysis;

import ac.uk.ncl.structure.Triple;
import com.google.common.collect.Multimap;

import java.text.MessageFormat;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class AnalysisUtils {

    public static void analyzeTripleMap(String header, Multimap<String, Triple> tripleMap) {
        int types = tripleMap.keySet().size();
        double avgInsCount = (double) tripleMap.size() / types;
        Map<String, Integer> instancesPerType = new HashMap<>();
        for (Map.Entry<String, Triple> entry : tripleMap.entries()) {
            if(!instancesPerType.containsKey(entry.getKey())) {
                instancesPerType.put(entry.getKey(), 1);
            } else {
                instancesPerType.put(entry.getKey(), instancesPerType.get(entry.getKey()) + 1);
            }
        }
        List<Map.Entry<String, Integer>> sortedTypeInsCounts =
                instancesPerType.entrySet().stream().sorted(Map.Entry.comparingByValue()).collect(Collectors.toList());
        int maxInsCount = sortedTypeInsCounts.get(sortedTypeInsCounts.size() - 1).getValue();
        String maxType = sortedTypeInsCounts.get(sortedTypeInsCounts.size() - 1).getKey();
        int minInsCount = sortedTypeInsCounts.get(0).getValue();
        String minType = sortedTypeInsCounts.get(0).getKey();
        int medianInsCount = sortedTypeInsCounts.get(sortedTypeInsCounts.size() / 2).getValue();

        System.out.println(MessageFormat.format("# {7} File Analysis\n" +
                "# Types = {0} | AvgInsCount = {1} | MedianInsCount = {6}\n" +
                "# MaxInsCount = {2} by {3}\n" +
                "# MinInsCount = {4} by {5}"
                , types
                , avgInsCount
                , maxInsCount
                , maxType
                , minInsCount
                , minType
                , medianInsCount
                , header));
    }
}
