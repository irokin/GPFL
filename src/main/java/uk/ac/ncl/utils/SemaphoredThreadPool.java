package uk.ac.ncl.utils;

import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.Semaphore;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class SemaphoredThreadPool extends ThreadPoolExecutor {
    Semaphore semaphore;

    public SemaphoredThreadPool(int nThreads) {
        super(nThreads, nThreads, 0L, TimeUnit.MILLISECONDS
                , new LinkedBlockingDeque<Runnable>(nThreads * 2));
        semaphore = new Semaphore(nThreads);
    }

    @Override
    public void execute(Runnable command) {
        try {
            semaphore.acquire();
            super.execute(command);
        } catch (InterruptedException e) {
            e.printStackTrace();
            System.exit(-1);
        }
    }

    @Override
    protected void afterExecute(Runnable r, Throwable t) {
        semaphore.release();
        super.afterExecute(r, t);
    }
}
