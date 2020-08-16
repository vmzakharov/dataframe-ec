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

        this.report(dataFrame1.rowCount(), stopwatch);

        stopwatch.start();
        DataFrame dataFrame2 = dataSet.loadAsDataFrame();
        stopwatch.stop();

        this.report(dataFrame2.rowCount(), stopwatch);

        stopwatch.start();
        DataFrame dataFrame3 = dataSet.loadAsDataFrame();
        stopwatch.stop();

        this.report(dataFrame3.rowCount(), stopwatch);
    }

    private void report(int rowCount, Stopwatch stopwatch)
    {
        System.out.printf("Loaded %,d rows\n", rowCount);
        System.out.printf("Elapsed time, ms: %,d, Memory used, bytes: %,d [T: %,d, F: %,d, U: %,d]\n",
                stopwatch.elapsedTimeMillis(),
                stopwatch.usedMemoryChangeBytes(),
                stopwatch.totalMemoryBytes(),
                stopwatch.freeMemoryBytes(),
                stopwatch.usedMemoryBytes()
        );
    }
}
