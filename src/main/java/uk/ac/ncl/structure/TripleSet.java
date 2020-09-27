package uk.ac.ncl.structure;

import com.google.common.collect.Multimap;
import com.google.common.collect.MultimapBuilder;
import uk.ac.ncl.Settings;

import java.util.Collection;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class TripleSet {
    public Set<Pair> trainPairs;
    public Set<Pair> validPairs;
    public Set<Pair> testPairs;

    public Set<TestQuery> testQueries = new HashSet<>();
    public Multimap<Long, TestQuery> headIndex = MultimapBuilder.hashKeys().hashSetValues().build();
    public Multimap<Long, TestQuery> tailIndex = MultimapBuilder.hashKeys().hashSetValues().build();

    public Set<Long> testHeads = new HashSet<>();
    public Set<Long> testTails = new HashSet<>();

    public TripleSet(Set<Pair> trainPairs, Set<Pair> validPairs, Set<Pair> testPairs) {
        this.trainPairs = trainPairs;
        this.validPairs = validPairs;
        this.testPairs = testPairs;

        testPairs.forEach(t -> {
            testHeads.add(t.subId);
            testTails.add(t.objId);

            TestQuery headQuery = new TestQuery(t, true);
            TestQuery tailQuery = new TestQuery(t, false);

            testQueries.add(headQuery);
            testQueries.add(tailQuery);

            headIndex.put(t.subId, tailQuery);
            tailIndex.put(t.objId, headQuery);
        });
    }

    public boolean inNonTest(Pair pair) {
        return trainPairs.contains(pair) || validPairs.contains(pair);
    }

    public boolean possibleSolution(Pair pair) {
        return testHeads.contains(pair.subId) || testTails.contains(pair.objId);
    }

    public void updateTestCases(Package p) {
        Multimap<TestQuery, Pair> distMap = MultimapBuilder.hashKeys().hashSetValues().build();
        for (Pair candidate : p.candidates) {
            for (TestQuery query : headIndex.get(candidate.subId)) {
                if (!testPairs.contains(candidate))
                    distMap.put(query, candidate);
                else if (candidate.equals(query.testPair)) {
                    distMap.put(query, candidate);
                }
            }
            for (TestQuery query : tailIndex.get(candidate.objId)) {
                if (!testPairs.contains(candidate))
                    distMap.put(query, candidate);
                else if (candidate.equals(query.testPair)) {
                    distMap.put(query, candidate);
                }
            }
        }

        for (Map.Entry<TestQuery, Collection<Pair>> entry : distMap.asMap().entrySet()) {
            entry.getKey().updateScoreList(new HashSet<>(entry.getValue()), p.rule);
        }

        for (TestQuery testQuery : testQueries) {
            if(!covered.contains(testQuery) && testQuery.covered())
                covered.add(testQuery);
        }

        covered.forEach(c -> {
            headIndex.remove(c.testPair.subId, c);
            tailIndex.remove(c.testPair.objId, c);
        });
    }

    public Set<TestQuery> covered = new HashSet<>();
    public int convergeRepeat = 0;
    public double coverage = 0;
    public boolean converge() {
        double currentCoverage = (double) covered.size() / testQueries.size();
        if(currentCoverage > 0.999) {
            coverage = currentCoverage;
            return true;
        }

        if(currentCoverage - coverage < 0.01 && coverage != 0)
            return ++convergeRepeat > Settings.COVER_REPEATS;
        else
            convergeRepeat = 0;

        coverage = currentCoverage;
        return false;
    }
}

