package com.cjh.lock;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.ForkJoinTask;
import java.util.concurrent.RecursiveTask;

/**
 * @author chenjiahao
 * @date 2021/8/12 11:18
 */

public class L3 {

    public static void main(String[] args) throws ExecutionException, InterruptedException {

        MyTask task = new MyTask(10, 1, 100);
        ForkJoinPool forkJoinPool = new ForkJoinPool();
        ForkJoinTask<Long> forkJoinTask = forkJoinPool.submit(task);
        Long aLong = forkJoinTask.get();
        System.out.println("aLong = " + aLong);
    }
}

class MyTask extends RecursiveTask<Long> {

    private long range;
    private long begin;
    private long end;
    private long result;

    public MyTask(long range, long begin, long end) {
        this.range = range;
        this.begin = begin;
        this.end = end;
    }

    @Override
    protected Long compute() {
        if (end - begin <= range) {
            for (long i = begin; i <= end; i++) {
                result += i;
            }
        } else {
            long mid = (end + begin) / 2;
            MyTask left = new MyTask(range, begin, mid);
            MyTask right = new MyTask(range, mid + 1, end);
            left.fork();
            right.fork();
            result = left.join() + right.join();
        }
        return result;
    }
}
