package io.github.vmzakharov.ecdataframe.dataframe.bench;

import io.github.vmzakharov.ecdataframe.dataframe.DataFrame;
import io.github.vmzakharov.ecdataframe.util.Stopwatch;
import org.eclipse.collections.api.list.MutableList;
import org.eclipse.collections.impl.factory.Lists;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.util.concurrent.CountDownLatch;

import static org.junit.jupiter.api.Assertions.*;

/*
 * These tests validate the thread safety of the data frame evaluation context
 */
public class DataFrameParallelTest
{
    private static final int ROW_COUNT = 5_000_000;
    private static final int THREAD_COUNT = 10;

//    private static final long EXPECTED = (long) ROW_COUNT * (ROW_COUNT - 1) / 2;
    private static final long EXPECTED = (long) ROW_COUNT * (ROW_COUNT - 1);
    private static DataFrame dataFrame;

    @BeforeAll
    public static void createDataFrame()
    {
        Stopwatch sw = new Stopwatch();
        sw.start();
        dataFrame = new DataFrame("Frame of Data")
                .addStringColumn("Foo").addLongColumn("Bar");

        for (int i = 0; i < ROW_COUNT; i++)
        {
            dataFrame.addRow(String.valueOf(i), i);
        }

        dataFrame.addColumn("TwoBars", "Bar + Bar");

        dataFrame.seal();
        sw.stop();
        System.out.println("Created data frame, " + sw.elapsedTimeMillis());
    }

    @Disabled
    @Test
    public void aggregateInBatchesManyTimes()
    throws InterruptedException
    {
        Stopwatch sw = new Stopwatch();
        sw.start();
        for (int attempt = 0; attempt < 100; attempt++)
        {
            CountDownLatch countDownLatch = new CountDownLatch(THREAD_COUNT);

            int batchSize = ROW_COUNT / THREAD_COUNT;

            MutableList<Summinator> workers = Lists.mutable.of();

            for (int i = 0; i < THREAD_COUNT; i++)
            {
                Summinator worker = new Summinator(i, dataFrame, i * batchSize, batchSize, countDownLatch);
                workers.add(worker);
                new Thread(worker).start();
            }

            countDownLatch.await();

            long result = workers.collectLong(Summinator::sum).sum();

            assertEquals(EXPECTED, result);
        }
        sw.stop();
        System.out.println("Total time: "  + sw.elapsedTimeMillis());
    }

    @Disabled
    @Test
    public void aggregateAllInParallel()
    throws InterruptedException
    {
        Stopwatch stopwatch = new Stopwatch();
        stopwatch.start();

        CountDownLatch countDownLatch = new CountDownLatch(THREAD_COUNT);

        MutableList<Summinator> workers = Lists.mutable.of();

        for (int i = 0; i < THREAD_COUNT; i++)
        {
            Summinator worker = new Summinator(i, dataFrame, 0, ROW_COUNT, countDownLatch);
            workers.add(worker);
            new Thread(worker).start();
        }

        countDownLatch.await();

        long result = workers.collectLong(Summinator::sum).sum();

        stopwatch.stop();
        assertEquals(EXPECTED, result / THREAD_COUNT);

        System.out.println("Parallel " + result + " (" + result / THREAD_COUNT + ") "  + stopwatch.elapsedTimeMillis());
    }

    @Disabled
    @Test
    public void aggregateInBatches()
    throws InterruptedException
    {
        Stopwatch stopwatch = new Stopwatch();
        stopwatch.start();

        CountDownLatch countDownLatch = new CountDownLatch(THREAD_COUNT);

        int batchSize = ROW_COUNT / THREAD_COUNT;

        MutableList<Summinator> workers = Lists.mutable.of();

        for (int i = 0; i < THREAD_COUNT; i++)
        {
            Summinator worker = new Summinator(i, dataFrame, i * batchSize, batchSize, countDownLatch);
            workers.add(worker);
            new Thread(worker).start();
        }

        countDownLatch.await();

        long result = workers.collectLong(Summinator::sum).sum();

        stopwatch.stop();

        assertEquals(EXPECTED, result);
        System.out.println("Threaded " + result + " " + stopwatch.elapsedTimeMillis());
    }

    // aggregate single threaded for performance comparison
    @Disabled
    @Test
    public void singleThreadAggregate()
    {
        Stopwatch stopwatch = new Stopwatch();
        stopwatch.start();
        Summinator worker = new Summinator(0, dataFrame, 0, ROW_COUNT, null);

        worker.run();

        long result = worker.sum();

        stopwatch.stop();
        assertEquals(EXPECTED, result);

        System.out.println("Regular  " + result + " " + stopwatch.elapsedTimeMillis());
    }

    private static class Summinator
    implements Runnable
    {
        private final int id;
        private final CountDownLatch countDownLatch;
        private final DataFrame dataFrame;
        private final int startIndex;
        private final int batchSize;
        private long sum = 0;

        public Summinator(int newId, DataFrame newDataFrame, int newStartIndex, int newBatchSize, CountDownLatch newCountDownLatch)
        {
            this.id = newId;
            this.countDownLatch = newCountDownLatch;
            this.dataFrame = newDataFrame;
            this.startIndex = newStartIndex;
            this.batchSize = newBatchSize;
        }

        @Override
        public void run()
        {
            System.out.println("Start " + this.id + " > " + this.startIndex);
            int endIndex = this.startIndex + this.batchSize;
            for (int i = this.startIndex; i < endIndex; i++)
            {
//                this.sum += this.dataFrame.getLong("Bar", i);
                this.sum += this.dataFrame.getLong("TwoBars", i);
            }

            if (this.countDownLatch != null)
            {
                this.countDownLatch.countDown();
            }

            System.out.println(" Done " + this.id + " > " + this.startIndex);
        }

        public long sum()
        {
            return this.sum;
        }
    }
}
