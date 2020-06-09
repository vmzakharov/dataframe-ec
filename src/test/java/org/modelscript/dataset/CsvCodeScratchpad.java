package org.modelscript.dataset;

import org.modelscript.dataframe.DataFrame;

public class CsvCodeScratchpad
{
    public static void main(String[] args)
    {
        System.out.println("Current working directory: "+System.getProperty("user.dir"));

        readStuff();
    }

    private static void readStuff()
    {
        CsvDataSet dataSet = new CsvDataSet("src/test/resources/employees.csv", "Employee");

        DataFrame dataFrame = dataSet.loadAsDataFrame();
        System.out.println(dataFrame.asCsvString());

        dataFrame.addDoubleColumn("Double Salary", "Salary*2");
        System.out.println(dataFrame.asCsvString());
    }
}
