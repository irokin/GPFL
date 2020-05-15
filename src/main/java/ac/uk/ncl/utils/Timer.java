package ac.uk.ncl.utils;

public class Timer {
    long initial;
    long lastStop;
    long tickCounts;
    long current;
    final long interval;
    final long stop;

    public Timer(long i, long s) {
        stop = s * 1000;
        interval = i * 1000;
    }

    public void start() {
        initial = System.currentTimeMillis();
        lastStop = initial;
    }

    public double elapsed() {
        return (System.currentTimeMillis() - initial) / 1000d;
    }

    public boolean continues() {
        return current - initial < stop;
    }

    public boolean tick() {
        current = System.currentTimeMillis();
        if(current - lastStop >= interval) {
            long tickSteps = (current - lastStop) / interval;
            tickCounts += tickSteps;
            lastStop += tickSteps * interval;
            return true;
        }
        return false;
    }

    public long getTickCounts() {
        return tickCounts;
    }
}
