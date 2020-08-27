package org.modelscript.dataset;

import org.junit.Test;
import org.modelscript.dataframe.DataFrame;
import org.modelscript.dataframe.DataFrameUtil;

import java.io.IOException;
import java.io.Reader;
import java.io.StringReader;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;

public class DataFrameLoadTest
{
    private String data = null;

    @Test
    public void loadData()
    {
        this.data = "Name,EmployeeId,HireDate,Dept,Salary\n" +
                "\"Alice\",1234,2020-01-01,\"Accounting\",110000.00\n" +
                "\"Bob\",1233,2010-01-01,\"Bee-bee-boo-boo\",100000.00\n" +
                "\"Carl\",10000,2005-11-21,\"Controllers\",130000.00\n" +
                "\"Diane\",10001,2012-09-20,\"\",130000.00\n" +
                "\"Ed\",10002,,,0.00"
                ;

        CsvDataSet dataSet = new StringBasedCsvDataSet("Foo", "Employees");

        DataFrame loaded = dataSet.loadAsDataFrame();

        DataFrame expected = new DataFrame("Expected")
                .addStringColumn("Name").addLongColumn("EmployeeId").addDateColumn("HireDate").addStringColumn("Dept").addDoubleColumn("Salary")
                .addRow("Alice", 1234, LocalDate.of(2020, 1, 1), "Accounting", 110000.0)
                .addRow("Bob", 1233, LocalDate.of(2010, 1, 1), "Bee-bee-boo-boo", 100000.0)
                .addRow("Carl", 10000, LocalDate.of(2005, 11, 21), "Controllers", 130000.0)
                .addRow("Diane", 10001, LocalDate.of(2012, 9, 20), "", 130000.0)
                .addRow("Ed", 10002, null, "", 0.0)
                ;

        DataFrameUtil.assertEquals(expected, loaded);
    }

    public class StringBasedCsvDataSet extends CsvDataSet
    {
        public StringBasedCsvDataSet(String dataFileName, String newName)
        {
            super(dataFileName, newName);
        }

        @Override
        protected Reader createReader()
        throws IOException
        {
            return new StringReader(DataFrameLoadTest.this.data);
        }
    }
}
