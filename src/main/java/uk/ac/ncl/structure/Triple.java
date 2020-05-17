package uk.ac.ncl.structure;

public class Triple {
    public String sub;
    public String pred;
    public String obj;

    public Triple(String sub, String pred, String obj) {
        this.sub = sub;
        this.pred = pred;
        this.obj = obj;
    }

    /**
     * Parse AnyBURL prediction: option = 0
     * GPFL: optoin = 1
     * @param line
     * @param option
     */
    public Triple(String line, int option) {
        if(option == 0) {
            String[] words = line.split("\\s");
            sub = words[0];
            pred = words[1];
            obj = words[2];
        } else if(option == 1) {
            String[] words = line.split(", ");
            sub = words[0].split("\\|")[1];
            pred = words[1];
            obj = words[2].split("\\|")[1].replaceAll("\\)", "");
        }
    }

    @Override
    public int hashCode() {
        return sub.hashCode() + pred.hashCode() + obj.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        if(obj instanceof Triple) {
            Triple right = (Triple) obj;
            return right.obj.equals(this.obj) && right.sub.equals(this.sub) && right.pred.equals(this.pred);
        }
        return false;
    }

    @Override
    public String toString() {
        return sub + "\t" + pred + "\t" + obj;
    }
}
