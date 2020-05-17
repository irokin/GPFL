package uk.ac.ncl.utils;
import java.util.List;
import java.util.function.Supplier;

public class MathUtils {
    public static double log(double x, double base) {
        return log( x ) / log( base );
    }

    public static double log(double x) {
        return Math.log( x );
    }

    public static double log(double x, Supplier<Double> supplier) {
        if ( x == 0.d) return supplier.get();
        return Math.log( x );
    }

    public static double entropy(double p, double offset) {
        return ( p == 0 || p == 1 ) ? offset
                : (- p * log(p , 2) - (1 - p) * log( (1 - p), 2));
    }

    public static double arrayMean(int[] ar) {
        int sum = 0;
        for(int i : ar) sum += i;
        return (double) sum / ar.length;
    }

    public static double listMean(List<Double> list) {
        double sum = 0;
        if(list.isEmpty()) return 0;
        for (double num : list) {
            sum += num;
        }
        return sum / list.size();
    }

    public static double listSum(List<Double> list) {
        double sum = 0;
        for (Double i : list) {
            sum += i;
        }
        return sum;
    }

    public static double arrayMean(double[] ar) {
        double sum = 0;
        for(double i : ar) sum += i;
        return sum / ar.length;
    }

    public static int[][] createIntervals(int size, int partitions) {
        int[][] intervals = new int[partitions][2];
        int interval = size / partitions;
        int residual = size % partitions;
        for(int i = 0; i < partitions; i++) {
            intervals[i][0] = interval * i;
            intervals[i][1] = interval * (i + 1);
            if(i == partitions - 1) intervals[i][1] += residual;
        }
        return intervals;
    }

    public static double STDEV(int[] ar) {
        double mean = arrayMean(ar);
        double sum = 0;
        for (int i : ar) sum += Math.pow(i - mean, 2);
        return Math.sqrt(sum / ar.length);
    }

    public static double STDEV(double[] ar) {
        double mean = arrayMean(ar);
        double sum = 0;
        for (double i : ar) sum += Math.pow(i - mean, 2);
        return Math.sqrt(sum / ar.length);
    }

    public static double sigmoid(double length, int type) {
        type = type == 2 ? 1 : type;
        double complexity = length + type;
        return ((1 / (1 + Math.exp(-complexity * 0.5))) - 0.5) * 2;
    }

    public static double sigmoid(double length) {
        return ((1 / (1 + Math.exp(-length * 0.5))) - 0.5) * 2;
    }
}
