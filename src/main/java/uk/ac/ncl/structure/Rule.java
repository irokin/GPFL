package uk.ac.ncl.structure;

import uk.ac.ncl.Settings;
import org.apache.commons.compress.utils.Lists;

import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.List;

public abstract class Rule {
    protected boolean closed;
    protected boolean fromSubject;
    public Atom head;
    public List<Atom> bodyAtoms;
    public RuleStats stats = new RuleStats();
    protected int type;

    Rule() {}

    Rule(Atom head, List<Atom> bodyAtoms) {
        this.head = head;
        this.bodyAtoms = new ArrayList<>(bodyAtoms);
        Atom lastAtom = bodyAtoms.get(bodyAtoms.size() - 1);
        closed = head.getSubjectId() == lastAtom.getObjectId() || head.getObjectId() == lastAtom.getObjectId();
        Atom firstAtom = bodyAtoms.get( 0 );
        fromSubject = head.getSubjectId() == firstAtom.getSubjectId();
    }

    public void setStats(double support, double totalPredictions, double groundTruth) {
        stats.support = support;
        stats.totalPredictions = totalPredictions;
        stats.groundTruth = groundTruth;
        stats.compute();
    }

    public void setStats(double support, double totalPredictions, double pcaTotalPredictions, double groundTruth) {
        stats.support = support;
        stats.totalPredictions = totalPredictions;
        stats.pcaTotalPredictions = pcaTotalPredictions;
        stats.groundTruth = groundTruth;
        stats.compute();
    }

    public void setStats(double support, double totalPredictions
            , double pcaTotalPredictions, double groundTruth
            , double validTotalPredictions, double validPredictions) {
        stats.support = support;
        stats.totalPredictions = totalPredictions;
        stats.pcaTotalPredictions = pcaTotalPredictions;
        stats.groundTruth = groundTruth;
        stats.validTotalPredictions = validTotalPredictions;
        stats.validPredictions = validPredictions;
        stats.compute();
    }

    public int length() {
        return bodyAtoms.size();
    }

    public boolean isClosed() {
        return closed;
    }

    public boolean isFromSubject() {
        return fromSubject;
    }

    public Atom copyHead() {
        return new Atom( head );
    }

    public List<Atom> copyBody() {
        List<Atom> result = Lists.newArrayList();
        bodyAtoms.forEach( atom -> result.add( new Atom(atom)));
        return result;
    }

    @Override
    public String toString() {
        String str = head + " <- ";
        List<String> atoms = new ArrayList<>();
        bodyAtoms.forEach( atom -> atoms.add(atom.toString()));
        return str + String.join(", ", atoms);
    }

    @Override
    public int hashCode() {
        int hashcode = head.hashCode();
        for(Atom atom : bodyAtoms) {
            hashcode += atom.hashCode();
        }
        return hashcode;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof Rule) {
            Rule right = (Rule) obj;
            return this.toString().equals(right.toString());
        }
        return false;
    }

    public double getQuality() {
        switch (Settings.QUALITY_MEASURE) {
            case "smoothedConf":
                return stats.smoothedConf;
            case "standardConf":
                return stats.standardConf;
            case "pcaConf":
                return stats.pcaConf;
            default:
                return stats.apcaConf;
        }
    }

    public double getQuality(String measure) {
        switch (measure) {
            case "smoothedConf":
                return stats.smoothedConf;
            case "standardConf":
                return stats.standardConf;
            case "pcaConf":
                return stats.pcaConf;
            default:
                return stats.apcaConf;
        }
    }

    public double getHeadCoverage() {
        return stats.headCoverage;
    }

    public double getStandardConf() {
        return stats.standardConf;
    }

    public double getSmoothedConf() {
        return stats.smoothedConf;
    }

    public double getPcaConf() {
        return stats.pcaConf;
    }

    public double getApcaConf() {
        return stats.apcaConf;
    }

    public double getValidPrecision() {
        return stats.validPrecision;
    }

    public double getPrecision() {
        return stats.precision;
    }

    public abstract long getTailAnchoring();

    public abstract long getHeadAnchoring();

    public int getType() {
        return type;
    }

    public Atom getBodyAtom(int i) {
        assert i < bodyAtoms.size();
        return bodyAtoms.get(i);
    }

    public static class RuleStats {
        public double support = 0d;
        public double groundTruth = 0d;
        public double totalPredictions = 0d;
        public double pcaTotalPredictions = 0d;
        public double validTotalPredictions = 0d;
        public double validPredictions = 0d;

        private double standardConf;
        private double smoothedConf;
        private double pcaConf;
        private double apcaConf;
        private double headCoverage;
        private double validPrecision;
        private double precision;

        @Override
        public String toString() {
            return MessageFormat.format("Support = {0}\nSC = {1}\nHC = {2}"
                    , String.valueOf(support)
                    , String.valueOf(smoothedConf)
                    , String.valueOf(headCoverage));
        }

        public void compute() {
            smoothedConf = support / (totalPredictions + Settings.CONFIDENCE_OFFSET);
            standardConf = totalPredictions == 0 ? 0 : support / totalPredictions;
            pcaConf = totalPredictions == 0 ? 0 : support / pcaTotalPredictions;
            headCoverage = groundTruth == 0 ? 0 : support / groundTruth;
            apcaConf = Settings.TARGET_FUNCTIONAL ? pcaConf : smoothedConf;
            validPrecision = validTotalPredictions == 0 ? 0 : validPredictions / validTotalPredictions;
        }

        public void setApcaConf(double apcaConf) {
            this.apcaConf = apcaConf;
        }

        public void setStandardConf(double standardConf) {
            this.standardConf = standardConf;
        }

        public void setSmoothedConf(double smoothedConf) {
            this.smoothedConf = smoothedConf;
        }

        public void setPcaConf(double pcaConf) {
            this.pcaConf = pcaConf;
        }

        public void setHeadCoverage(double headCoverage) {
            this.headCoverage = headCoverage;
        }

        public void setValidPrecision(double validPrecision) {
            this.validPrecision = validPrecision;
        }

        public void setPrecision(double precision) {
            this.precision = precision;
        }

    }
}
