package uk.ac.ncl.structure;

import uk.ac.ncl.Settings;
import uk.ac.ncl.core.Context;
import uk.ac.ncl.core.GlobalTimer;
import uk.ac.ncl.core.GraphOps;
import uk.ac.ncl.utils.IO;
import com.google.common.collect.Multimap;
import com.google.common.collect.MultimapBuilder;
import com.google.common.collect.Sets;
import org.neo4j.graphdb.GraphDatabaseService;

import java.text.DecimalFormat;
import java.util.*;
import java.util.concurrent.BlockingQueue;

public class Template extends Rule {
    public List<SimpleInsRule> insRules = new ArrayList<>();

    public Template(String line) {
        String[] words = line.split("\t");
        closed = words[1].equals("CAR");
        String headline = words[2].split(" <- ")[0];
        String[] bodyLines = words[2].split(" <- ")[1].split(", ");
        head = new Atom(headline, true);
        bodyAtoms = new ArrayList<>();
        for (String bodyLine : bodyLines) {
            bodyAtoms.add(new Atom(bodyLine, false));
        }
        Atom firstAtom = bodyAtoms.get(0);
        fromSubject = firstAtom.subject.equals("X");
    }

    public Template(Atom h, List<Atom> b) {
        super( h, b );
        Atom firstAtom = bodyAtoms.get( 0 );
        Atom lastAtom = bodyAtoms.get( bodyAtoms.size() - 1 );
        head.subject = "X";
        head.object = "Y";

        int variableCount = 0;
        for(Atom atom : bodyAtoms) {
            atom.subject = "V" + variableCount;
            atom.object = "V" + ++variableCount;
        }

        if ( fromSubject ) firstAtom.subject = "X";
        else firstAtom.subject = "Y";

        if ( closed && fromSubject ) lastAtom.object = "Y";
        else if ( closed ) lastAtom.object = "X";
    }

    @Override
    public long getTailAnchoring() {
        return -1;
    }

    @Override
    public long getHeadAnchoring() {
        return -1;
    }

    public void specialization(GraphDatabaseService graph, Set<Pair> groundTruth, Set<Pair> validPair
            , Multimap<Long, Long> anchoringToOriginal, Multimap<Long, Long> validOriginals
            , Context context
            , BlockingQueue<String> ruleFileContents
            , BlockingQueue<String> indexFileContents) throws InterruptedException {
        DecimalFormat f = new DecimalFormat("####.#####");
        List<String> contents = new ArrayList<>();
        CountedSet<Pair> bodyGroundings = GraphOps.bodyGroundingCoreAPI(graph, this
                , false, GlobalTimer::stopSpec);
        if(context.checkInsRuleCap() || GlobalTimer.stopSpec()) return;

        if(closed) {
             if(evalClosedRule(bodyGroundings, groundTruth, validPair)) {
                 context.addSpecializedRules(this);
                 indexFileContents.put("ABS: " + context.getIndex(this) + "\t"
                         + this.toRuleIndexString() + "\t"
                         + f.format(getStandardConf()) + "\t"
                         + f.format(getSmoothedConf()) + "\t"
                         + f.format(getPcaConf()) + "\t"
                         + f.format(getApcaConf()) + "\t"
                         + f.format(getHeadCoverage()) + "\t"
                         + f.format(getValidPrecision()) + "\n");
                 ruleFileContents.put(this.toString() + "\t"
                         + f.format(getQuality()) + "\t"
                         + f.format(getHeadCoverage()) + "\t"
                         + f.format(getValidPrecision()) + "\t"
                         + (int) stats.support + "\t"
                         + (int) stats.totalPredictions);
             }
        }
        else {
            stats.groundTruth = groundTruth.size();
            Multimap<Long, Long> originalToTail = MultimapBuilder.hashKeys().hashSetValues().build();
            Multimap<Long, Long> tailToOriginal = MultimapBuilder.hashKeys().hashSetValues().build();
            for (Pair bodyGrounding : bodyGroundings) {
                originalToTail.put(bodyGrounding.subId, bodyGrounding.objId);
                tailToOriginal.put(bodyGrounding.objId, bodyGrounding.subId);
            }
            Set<Long> groundingOriginals = originalToTail.keySet();

            for (Long anchoring : anchoringToOriginal.keySet()) {
                if(context.checkInsRuleCap() || GlobalTimer.stopSpec()) break;

                Set<Pair> visited = new HashSet<>();
                String headName = (String) graph.getNodeById(anchoring).getProperty(Settings.NEO4J_IDENTIFIER);
                Rule HAR = new InstantiatedRule(this, headName, anchoring);
                if(evaluateRule(HAR, anchoringToOriginal.get(anchoring), validOriginals.get(anchoring), groundingOriginals)) {
                    stats.support += HAR.stats.support;
                    stats.totalPredictions += HAR.stats.totalPredictions;
                    stats.pcaTotalPredictions += HAR.stats.pcaTotalPredictions;
                    context.updateTotalInsRules();
                    contents.add("0" + ","
                            + HAR.getHeadAnchoring() + ","
                            + f.format(HAR.getStandardConf()) + ","
                            + f.format(HAR.getSmoothedConf()) + ","
                            + f.format(HAR.getPcaConf()) + ","
                            + f.format(HAR.getApcaConf()) + ","
                            + f.format(HAR.getHeadCoverage()) + ","
                            + f.format(HAR.getValidPrecision()));
                    ruleFileContents.put(HAR.toString() + "\t"
                            + f.format(HAR.getQuality()) + "\t"
                            + f.format(HAR.getHeadCoverage()) + "\t"
                            + f.format(HAR.getValidPrecision()) + "\t"
                            + (int) HAR.stats.support + "\t"
                            + (int) HAR.stats.totalPredictions);
                }

                for (Long original : anchoringToOriginal.get(anchoring)) {
                    if(context.checkInsRuleCap() || GlobalTimer.stopSpec()) break;

                    for (Long tail : originalToTail.get(original)) {
                        if(context.checkInsRuleCap() || GlobalTimer.stopSpec()) break;
                        Pair candidate = new Pair(anchoring, tail);
                        if(!visited.contains(candidate) && !trivialCheck(anchoring, tail)) {
                            visited.add(new Pair(anchoring, tail));
                            candidate.subName = headName;
                            candidate.objName = (String) graph.getNodeById(tail).getProperty(Settings.NEO4J_IDENTIFIER);
                            Rule BAR = new InstantiatedRule(this, candidate);
                            if (evaluateRule(BAR, anchoringToOriginal.get(anchoring), validOriginals.get(anchoring), tailToOriginal.get(tail))) {
                                context.updateTotalInsRules();
                                contents.add("2" + ","
                                        + BAR.getHeadAnchoring() + ","
                                        + BAR.getTailAnchoring() + ","
                                        + f.format(BAR.getStandardConf()) + ","
                                        + f.format(BAR.getSmoothedConf()) + ","
                                        + f.format(BAR.getPcaConf()) + ","
                                        + f.format(BAR.getApcaConf()) + ","
                                        + f.format(BAR.getHeadCoverage()) + ","
                                        + f.format(BAR.getValidPrecision()));
                                ruleFileContents.put(BAR.toString() + "\t"
                                        + f.format(BAR.getQuality()) + "\t"
                                        + f.format(BAR.getHeadCoverage()) + "\t"
                                        + f.format(BAR.getValidPrecision()) + "\t"
                                        + (int) BAR.stats.support + "\t"
                                        + (int) BAR.stats.totalPredictions);
                            }
                        }
                    }
                }
            }

            stats.compute();
            if(!contents.isEmpty()) {
                context.addSpecializedRules(this);
                indexFileContents.put("ABS: " + context.getIndex(this) + "\t"
                        + this.toRuleIndexString() + "\t"
                        + f.format(getStandardConf()) + "\t"
                        + f.format(getSmoothedConf()) + "\t"
                        + f.format(getPcaConf()) + "\t"
                        + f.format(getApcaConf()) + "\t"
                        + f.format(getHeadCoverage()) + "\t"
                        + f.format(getValidPrecision()) + "\n"
                        + String.join("\t", contents) + "\n");
            }
        }
    }

    public void applyRule(GraphDatabaseService graph, Context context) {
        CountedSet<Pair> bodyGroundings = GraphOps.bodyGroundingCoreAPI(graph, this, true, context::checkSuggestionCap);
        Set<Long> originals = Sets.newHashSet();
        Multimap<Long, Long> tailToOriginals = MultimapBuilder.hashKeys().hashSetValues().build();
        for (Pair grounding : bodyGroundings) {
            originals.add(grounding.subId);
            tailToOriginals.put(grounding.objId, grounding.subId);
        }

        if (closed) applyClosedRule(bodyGroundings, context);
        else {
            assert !insRules.isEmpty();
            insRules.sort(IO.ruleComparatorBySC());
            for (SimpleInsRule rule : insRules) {
                if(context.checkSuggestionCap())
                    break;

                rule.insRuleString(graph);
                if (rule.type == 0)
                    applyHeadAnchoredRules(rule, originals, context);
                else if (rule.type == 2)
                    applyBothAnchoredRules(rule, tailToOriginals, context);
            }
            insRules.clear();
        }
    }

    private boolean evalClosedRule(CountedSet<Pair> bodyGroundings, Set<Pair> groundTruth, Set<Pair> validPair) {
        double totalPrediction = 0, correctPrediction = 0, pcaTotalPrediction = 0
                , validTotalPredictions = 0, validPredictions = 0;

        Set<Long> subs = new HashSet<>();
        for (Pair pair : groundTruth)
            subs.add(pair.subId);

        for (Pair grounding : bodyGroundings) {
            long sub = fromSubject ? grounding.subId : grounding.objId;
            if(subs.contains(sub))
                pcaTotalPrediction++;

            Pair prediction = fromSubject ? grounding : new Pair(grounding.objId, grounding.subId);
            if(groundTruth.contains(prediction))
                correctPrediction++;
            else {
                validTotalPredictions++;
                if(validPair.contains(prediction))
                    validPredictions++;
            }

            totalPrediction++;
        }
        setStats(correctPrediction, totalPrediction, pcaTotalPrediction
                , groundTruth.size(), validTotalPredictions, validPredictions);
        return qualityCheck(this);
    }

    private boolean evaluateRule(Rule rule, Collection<Long> originals, Collection<Long> validOriginals, Collection<Long> groundingOriginals) {
        int totalPrediction = 0, support = 0, groundTruth = originals.size()
                , validTotalPredictions = 0, validPredictions = 0;
        for (Long groundingOriginal : groundingOriginals) {
            totalPrediction++;
            if(originals.contains(groundingOriginal))
                support++;
            else {
                validTotalPredictions++;
                if(validOriginals.contains(groundingOriginal))
                    validPredictions++;
            }
        }
        int pcaTotalPredictions = isFromSubject() ? support : totalPrediction;
        rule.setStats(support, totalPrediction, pcaTotalPredictions, groundTruth, validTotalPredictions, validPredictions);
        return qualityCheck(rule);
    }

    public static boolean qualityCheck(Rule rule) {
        return (rule.stats.support >= Settings.SUPPORT)
                && (rule.getQuality() >= Settings.CONF)
                && (rule.getHeadCoverage() >= Settings.HEAD_COVERAGE);
    }

    /**
     * Remove the both anchored trivial rule taking the form R(e,Y) <- R(Y,e).
     */
    private boolean trivialCheck(long head, long tail) {
        if(Settings.ALLOW_INS_REVERSE)
            return false;
        return head == tail && length() == 1 && this.head.predicate.equals(bodyAtoms.get(0).predicate);
    }

    private void applyHeadAnchoredRules(SimpleInsRule rule, Set<Long> originals, Context context) {
        for (Long original : originals) {
            Pair pair = fromSubject ? new Pair(original, rule.headAnchoringId) : new Pair(rule.headAnchoringId, original);
            if(!pair.isSelfloop()) {
                context.putInPredictionMap(pair, rule);
            }
        }
    }

    private void applyBothAnchoredRules(SimpleInsRule rule, Multimap<Long, Long> tailToOriginals, Context context) {
        for (Long original : tailToOriginals.get(rule.tailAnchoringId)) {
            Pair pair = fromSubject ? new Pair(original, rule.headAnchoringId) : new Pair(rule.headAnchoringId, original);
            if(!pair.isSelfloop()) {
                context.putInPredictionMap(pair, rule);
            }
        }
    }

    private void applyClosedRule(CountedSet<Pair> bodyGroundings, Context context) {
        for (Pair grounding : bodyGroundings) {
            Pair pair = fromSubject ? grounding : new Pair(grounding.objId, grounding.subId);
            if(!pair.isSelfloop()) {
                context.putInPredictionMap(grounding, this);
            }
        }
    }

    @Override
    public String toString() {
        String header = isClosed() ? "CAR\t" : "OAR\t";
        return header + super.toString();
    }

    public String toRuleIndexString() {
        String str = isClosed() ? "CAR\t" : "OAR\t";
        str += head + " <- ";
        List<String> atoms = new ArrayList<>();
        bodyAtoms.forEach( atom -> atoms.add(atom.toRuleIndexString()));
        return str + String.join(", ", atoms);
    }
}
