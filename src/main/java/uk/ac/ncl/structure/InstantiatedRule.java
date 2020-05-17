package uk.ac.ncl.structure;

public class InstantiatedRule extends Rule {

    public InstantiatedRule(Rule base, String headAnchoring, long id) {
        super(base.copyHead(), base.copyBody());
        this.type = 0;
        if ( fromSubject ) {
            head.setObject(headAnchoring);
            head.setObjectId(id);
        }
        else {
            head.setSubject(headAnchoring);
            head.setSubjectId(id);
        }
    }

    public InstantiatedRule(Rule base, Pair candidate) {
        super(base.copyHead(), base.copyBody());
        this.type = 2;
        if ( fromSubject ) {
            head.setObject(candidate.subName);
            head.setObjectId(candidate.subId);
        }
        else {
            head.setSubject(candidate.subName);
            head.setSubjectId(candidate.subId);
        }
        bodyAtoms.get(bodyAtoms.size() - 1).setObject(candidate.objName);
        bodyAtoms.get(bodyAtoms.size() - 1).setObjectId(candidate.objId);
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        if(type == 0) sb.append("HAR\t");
        else if(type == 1) sb.append("TAR\t");
        else sb.append("BAR\t");
        return sb.append(super.toString()).toString();
    }

    public long getTailAnchoring() {
        return bodyAtoms.get(bodyAtoms.size() - 1).getObjectId();
    }

    public long getHeadAnchoring() {
        return isFromSubject() ? head.getObjectId() : head.getSubjectId();
    }
}
