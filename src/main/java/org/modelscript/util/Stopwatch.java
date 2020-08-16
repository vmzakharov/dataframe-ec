package org.modelscript.util;

public class Stopwatch
{
    private long startTimeMillis;
    private long endTimeMillis;

    private long startUsedMemoryBytes;
    private long endUsedMemoryBytes;

    public void start()
    {
        this.startUsedMemoryBytes = this.usedMemoryBytes();
        this.startTimeMillis = System.currentTimeMillis();
    }

    public void stop()
    {
        this.endTimeMillis = System.currentTimeMillis();
        this.endUsedMemoryBytes = this.usedMemoryBytes();
    }

    public long elapsedTimeMillis()
    {
        return this.endTimeMillis - this.startTimeMillis;
    }

    public long usedMemoryChangeBytes()
    {
        return this.endUsedMemoryBytes - this.startUsedMemoryBytes;
    }

    private long usedMemoryBytes()
    {
        System.gc();
        System.gc();
        System.gc();
        Runtime runtime = Runtime.getRuntime();
        return runtime.totalMemory() - runtime.freeMemory();
    }
}
