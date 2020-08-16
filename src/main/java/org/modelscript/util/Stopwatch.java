package org.modelscript.util;

public class Stopwatch
{
    private long startTimeMillis;
    private long endTimeMillis;

    private long startUsedMemoryBytes;
    private long endUsedMemoryBytes;

    private long totalMemoryBytes;
    private long usedMemoryBytes;
    private long freeMemoryBytes;

    public void start()
    {
        this.recordMemoryUsage();
        this.startUsedMemoryBytes = this.usedMemoryBytes;
        this.startTimeMillis = System.currentTimeMillis();
    }

    public void stop()
    {
        this.endTimeMillis = System.currentTimeMillis();
        this.recordMemoryUsage();
        this.endUsedMemoryBytes = this.usedMemoryBytes;
    }

    public long elapsedTimeMillis()
    {
        return this.endTimeMillis - this.startTimeMillis;
    }

    public long usedMemoryChangeBytes()
    {
        return this.endUsedMemoryBytes - this.startUsedMemoryBytes;
    }

    public long totalMemoryBytes()
    {
        return this.totalMemoryBytes;
    }

    public long usedMemoryBytes()
    {
        return this.usedMemoryBytes;
    }

    public long freeMemoryBytes()
    {
        return this.freeMemoryBytes;
    }

    private void recordMemoryUsage()
    {
        System.gc();
        System.gc();
        System.gc();
        Runtime runtime = Runtime.getRuntime();

        this.freeMemoryBytes = runtime.freeMemory();
        this.totalMemoryBytes = runtime.totalMemory();
        this.usedMemoryBytes = this.totalMemoryBytes - this.freeMemoryBytes;
    }
}
