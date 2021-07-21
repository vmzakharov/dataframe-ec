package io.github.vmzakharov.ecdataframe.dataset;

import io.github.vmzakharov.ecdataframe.dataframe.DataFrame;
import io.github.vmzakharov.ecdataframe.util.Stopwatch;

import java.nio.file.Paths;

public class CsvCodeScratchpad
{
    public static void main(String[] args)
    {
        System.out.println("Current working directory: " + System.getProperty("user.dir"));

        CsvCodeScratchpad csvCodeScratchpad = new CsvCodeScratchpad();

        csvCodeScratchpad.readStuff();
    }

    private void readStuff()
    {
        String fileName = "src/test/resources/employees.csv";
        String fileNameOut = "src/test/resources/employees_out.csv";
//        String fileName = "src/test/resources/employees_1mm.csv";
//        String fileNameOut = "src/test/resources/employees_1mm_out.csv";

//        CsvDataSet dataSet = new CsvDataSet(fileName, "Employee");
        CsvDataSet dataSet = new CsvDataSet(Paths.get(fileName), "Employee");
        dataSet.convertEmptyElementsToNulls();

        Stopwatch stopwatch = new Stopwatch();
        stopwatch.start();
        stopwatch.stop();

        stopwatch.start();
        DataFrame dataFrame1 = dataSet.loadAsDataFrame();
        stopwatch.stop();
        this.report("Loaded", dataFrame1.rowCount(), stopwatch);

        stopwatch.start();
        CsvDataSet dataSetOut = new CsvDataSet(Paths.get(fileNameOut), "Employee");
        dataSetOut.write(dataFrame1);
        stopwatch.stop();
        this.report("Wrote", dataFrame1.rowCount(), stopwatch);

//        stopwatch.start();
//        DataFrame dataFrame2 = dataSet.loadAsDataFrame();
//        stopwatch.stop();
//        this.report("Loaded", dataFrame2.rowCount(), stopwatch);
//
//        stopwatch.start();
//        DataFrame dataFrame3 = dataSet.loadAsDataFrame();
//        stopwatch.stop();
//        this.report("Loaded", dataFrame3.rowCount(), stopwatch);
    }

    private void report(String message, int rowCount, Stopwatch stopwatch)
    {
        System.out.printf(message + ": %,d rows\n", rowCount);
        System.out.printf("Elapsed time, ms: %,d, Memory used, bytes: %,d [T: %,d, F: %,d, U: %,d]\n",
                stopwatch.elapsedTimeMillis(),
                stopwatch.usedMemoryChangeBytes(),
                stopwatch.totalMemoryBytes(),
                stopwatch.freeMemoryBytes(),
                stopwatch.usedMemoryBytes()
        );
    }
}
