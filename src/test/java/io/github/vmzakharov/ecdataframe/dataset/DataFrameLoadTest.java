package io.github.vmzakharov.ecdataframe.dataset;

import io.github.vmzakharov.ecdataframe.dataframe.DataFrame;
import org.junit.Test;
import io.github.vmzakharov.ecdataframe.dataframe.DataFrameUtil;

import java.io.IOException;
import java.io.Reader;
import java.io.StringReader;
import java.time.LocalDate;

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

    @Test
    public void dateParsingFormat1()
    {
        this.data = "Name,Date\n" +
                "\"Alice\",2020-01-01\n" +
                "\"Bob\",  2010-1-01\n" +
                "\"Carl\", 2005-11-21\n" +
                "\"Diane\",2012-09-2\n" +
                "\"Ed\","
        ;

        CsvDataSet dataSet = new StringBasedCsvDataSet("Foo", "Dates");

        DataFrame loaded = dataSet.loadAsDataFrame();

        DataFrame expected = new DataFrame("Expected")
                .addStringColumn("Name").addDateColumn("Date")
                .addRow("Alice", LocalDate.of(2020,  1,  1))
                .addRow("Bob",   LocalDate.of(2010,  1,  1))
                .addRow("Carl",  LocalDate.of(2005, 11, 21))
                .addRow("Diane", LocalDate.of(2012,  9,  2))
                .addRow("Ed",    null)
                ;

        DataFrameUtil.assertEquals(expected, loaded);
    }

    @Test
    public void dateParsingFormat2()
    {
        this.data = "Name,Date\n" +
                "\"Alice\",2020/01/01\n" +
                "\"Bob\",  2010/1/01\n" +
                "\"Carl\", 2005/11/21\n" +
                "\"Diane\",2012/09/2\n" +
                "\"Ed\","
        ;

        CsvDataSet dataSet = new StringBasedCsvDataSet("Foo", "Dates");

        DataFrame loaded = dataSet.loadAsDataFrame();

        DataFrame expected = new DataFrame("Expected")
                .addStringColumn("Name").addDateColumn("Date")
                .addRow("Alice", LocalDate.of(2020,  1,  1))
                .addRow("Bob",   LocalDate.of(2010,  1,  1))
                .addRow("Carl",  LocalDate.of(2005, 11, 21))
                .addRow("Diane", LocalDate.of(2012,  9,  2))
                .addRow("Ed",    null)
                ;

        DataFrameUtil.assertEquals(expected, loaded);
    }

    @Test
    public void dateParsingFormat3()
    {
        this.data = "Name,Date\n" +
                "\"Alice\",01/01/2020\n" +
                "\"Bob\",  1/01/2010\n" +
                "\"Carl\", 11/21/2005\n" +
                "\"Diane\",09/2/2012\n" +
                "\"Ed\","
        ;

        CsvDataSet dataSet = new StringBasedCsvDataSet("Foo", "Dates");

        DataFrame loaded = dataSet.loadAsDataFrame();

        DataFrame expected = new DataFrame("Expected")
                .addStringColumn("Name").addDateColumn("Date")
                .addRow("Alice", LocalDate.of(2020,  1,  1))
                .addRow("Bob",   LocalDate.of(2010,  1,  1))
                .addRow("Carl",  LocalDate.of(2005, 11, 21))
                .addRow("Diane", LocalDate.of(2012,  9,  2))
                .addRow("Ed",    null)
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
