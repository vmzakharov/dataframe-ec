package org.modelscript.dataset;

import org.modelscript.dataframe.DataFrame;
import org.modelscript.util.Stopwatch;

public class CsvCodeScratchpad
{
    public static void main(String[] args)
    {
        System.out.println("Current working directory: "+System.getProperty("user.dir"));

        CsvCodeScratchpad csvCodeScratchpad = new CsvCodeScratchpad();

        csvCodeScratchpad.readStuff();
    }

    private void readStuff()
    {
//        String fileName = "src/test/resources/employees.csv";
        String fileName = "src/test/resources/employees_1mm.csv";
        CsvDataSet dataSet = new CsvDataSet(fileName, "Employee");

        Stopwatch stopwatch = new Stopwatch();
        stopwatch.start();
        stopwatch.stop();

        stopwatch.start();
        DataFrame dataFrame1 = dataSet.loadAsDataFrame();
        stopwatch.stop();

        this.report("Loaded " + dataFrame1.rowCount() + " rows", stopwatch);

        stopwatch.start();
        DataFrame dataFrame2 = dataSet.loadAsDataFrame();
        stopwatch.stop();

        this.report("Loaded " + dataFrame2.rowCount() + " rows", stopwatch);

        stopwatch.start();
        DataFrame dataFrame3 = dataSet.loadAsDataFrame();
        stopwatch.stop();

        this.report("Loaded " + dataFrame3.rowCount() + " rows", stopwatch);
    }

    private void report(String message, Stopwatch stopwatch)
    {
        System.out.println(message);
        System.out.printf("Elapsed time, ms: %,d, Memory used, bytes: %,d\n", stopwatch.elapsedTimeMillis(), stopwatch.usedMemoryChangeBytes());
    }
}
