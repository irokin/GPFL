package uk.ac.ncl.structure;

import com.google.common.collect.Multimap;
import com.google.common.collect.MultimapBuilder;
import uk.ac.ncl.Settings;
import uk.ac.ncl.utils.IO;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class TestQuery {
    public Pair testPair;
    public boolean headQuery;
    public ScoreList<Pair> scoreList;

    public Multimap<Pair, Rule> pairRuleMap = MultimapBuilder.hashKeys().hashSetValues().build();

    public TestQuery(Pair testPair, boolean headQuery) {
        this.testPair = testPair;
        this.headQuery = headQuery;
        scoreList = new ScoreList<>();
    }

    public void updateScoreList(Set<Pair> candidates, Rule r) {
        Set<Pair> filterCandidates = new HashSet<>();
        Set<Pair> targetGroup = getTargetGroup();
        if(targetGroup.size() > 1) {
            candidates.forEach(c -> {
                if(targetGroup.contains(c)) filterCandidates.add(c);
            });
        } else {
            filterCandidates.addAll(candidates);
        }

        filterCandidates.forEach(c -> pairRuleMap.put(c, r));
        scoreList.add(filterCandidates);
    }

    public Set<Pair> getTargetGroup() {
        for (Set<Pair> group : scoreList.getList())
            if(group.contains(testPair)) return group;
        return new HashSet<>();
    }

    public boolean covered() {
        int countBeforeTestCase = 0;
        for (Set<Pair> group : scoreList.getList()) {
            if(group.contains(testPair)) {
                return group.size() == 1 || countBeforeTestCase > Settings.TOP_K;
            } else {
                countBeforeTestCase += group.size();
            }
        }
        return countBeforeTestCase > Settings.TOP_K;
    }

    public List<Pair> getTopPairs(int k) {
        List<Pair> list = new ArrayList<>();
        for (Set<Pair> s : scoreList.getList()) {
            for (Pair pair : s) {
                if(list.size() < k && !list.contains(pair)) {
                    list.add(pair);
                    if(list.size() >= k)
                        break;
                }
            }
        }
        return list;
    }

    public List<Rule> getSuggestingRules(Pair p) {
        List<Rule> rules = new ArrayList<>(pairRuleMap.get(p));
        rules.sort(IO.ruleComparatorBySC(Settings.QUALITY_MEASURE));
        return rules;
    }
}
