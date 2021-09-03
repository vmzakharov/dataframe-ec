package io.github.vmzakharov.ecdataframe.dataset;

import io.github.vmzakharov.ecdataframe.dataframe.DataFrame;
import io.github.vmzakharov.ecdataframe.dataframe.DataFrameUtil;
import io.github.vmzakharov.ecdataframe.dsl.value.ValueType;
import org.eclipse.collections.api.map.MutableMap;
import org.eclipse.collections.impl.block.factory.Functions;
import org.junit.Assert;
import org.junit.Test;

import java.time.LocalDate;

public class DataFrameLoadTest
{
    @Test
    public void loadData()
    {
        CsvDataSet dataSet = new StringBasedCsvDataSet("Foo", "Employees",
                "Name,EmployeeId,HireDate,Dept,Salary\n"
                        + "\"Alice\",1234,2020-01-01,\"Accounting\",110000.00\n"
                        + "\"Bob\",1233,2010-01-01,\"Bee-bee-boo-boo\",100000.00\n"
                        + "\"Carl\",10000,2005-11-21,\"Controllers\",130000.00\n"
                        + "\"Diane\",10001,2012-09-20,\"\",130000.00\n"
                        + "\"Ed\",10002,,,0.00"
        );

        DataFrame loaded = dataSet.loadAsDataFrame();

        DataFrame expected = new DataFrame("Expected")
                .addStringColumn("Name").addLongColumn("EmployeeId").addDateColumn("HireDate").addStringColumn("Dept").addDoubleColumn("Salary")
                .addRow("Alice", 1234, LocalDate.of(2020, 1, 1), "Accounting", 110000.0)
                .addRow("Bob", 1233, LocalDate.of(2010, 1, 1), "Bee-bee-boo-boo", 100000.0)
                .addRow("Carl", 10000, LocalDate.of(2005, 11, 21), "Controllers", 130000.0)
                .addRow("Diane", 10001, LocalDate.of(2012, 9, 20), "", 130000.0)
                .addRow("Ed", 10002, null, "", 0.0);

        DataFrameUtil.assertEquals(expected, loaded);
    }

    @Test
    public void dateParsingFormat1()
    {
        CsvDataSet dataSet = new StringBasedCsvDataSet("Foo", "Dates",
                "Name,Date\n"
                        + "\"Alice\",2020-01-01\n"
                        + "\"Bob\",  2010-1-01\n"
                        + "\"Carl\", 2005-11-21\n"
                        + "\"Diane\",2012-09-2\n"
                        + "\"Ed\","
        );

        DataFrame loaded = dataSet.loadAsDataFrame();

        DataFrame expected = new DataFrame("Expected")
                .addStringColumn("Name").addDateColumn("Date")
                .addRow("Alice", LocalDate.of(2020, 1, 1))
                .addRow("Bob", LocalDate.of(2010, 1, 1))
                .addRow("Carl", LocalDate.of(2005, 11, 21))
                .addRow("Diane", LocalDate.of(2012, 9, 2))
                .addRow("Ed", null);

        DataFrameUtil.assertEquals(expected, loaded);
    }

    @Test
    public void dateParsingFormat2()
    {
        CsvDataSet dataSet = new StringBasedCsvDataSet("Foo", "Dates",
                "Name,Date\n"
                        + "\"Alice\",2020/01/01\n"
                        + "\"Bob\",  2010/1/01\n"
                        + "\"Carl\", 2005/11/21\n"
                        + "\"Diane\",2012/09/2\n"
                        + "\"Ed\","
        );

        DataFrame loaded = dataSet.loadAsDataFrame();

        DataFrame expected = new DataFrame("Expected")
                .addStringColumn("Name").addDateColumn("Date")
                .addRow("Alice", LocalDate.of(2020, 1, 1))
                .addRow("Bob", LocalDate.of(2010, 1, 1))
                .addRow("Carl", LocalDate.of(2005, 11, 21))
                .addRow("Diane", LocalDate.of(2012, 9, 2))
                .addRow("Ed", null);

        DataFrameUtil.assertEquals(expected, loaded);
    }

    @Test
    public void dateParsingFormat3()
    {
        CsvDataSet dataSet = new StringBasedCsvDataSet("Foo", "Dates",
                  "Name,Date\n"
                + "\"Alice\",01/01/2020\n"
                + "\"Bob\",  1/01/2010\n"
                + "\"Carl\", 11/21/2005\n"
                + "\"Diane\",09/2/2012\n"
                + "\"Ed\","
        );

        DataFrame loaded = dataSet.loadAsDataFrame();

        DataFrame expected = new DataFrame("Expected")
                .addStringColumn("Name").addDateColumn("Date")
                .addRow("Alice", LocalDate.of(2020, 1, 1))
                .addRow("Bob", LocalDate.of(2010, 1, 1))
                .addRow("Carl", LocalDate.of(2005, 11, 21))
                .addRow("Diane", LocalDate.of(2012, 9, 2))
                .addRow("Ed", null);

        DataFrameUtil.assertEquals(expected, loaded);
    }

    @Test
    public void loadDataWithSchema()
    {
        CsvSchema schema = new CsvSchema();
        schema.addColumn("Name", ValueType.STRING);
        schema.addColumn("EmployeeId", ValueType.LONG);
        schema.addColumn("HireDate", ValueType.DATE, "uuuu-M-d");
        schema.addColumn("Dept", ValueType.STRING);
        schema.addColumn("Salary", ValueType.DOUBLE);

        CsvDataSet dataSet = new StringBasedCsvDataSet("Foo", "Employees", schema,
                "Name,EmployeeId,HireDate,Dept,Salary\n"
                        + "\"Alice\",1234,2020-01-01,\"Accounting\",110000.00\n"
                        + "\"Bob\",1233,2010-01-01,\"Bee-bee-boo-boo\",100000.00\n"
                        + "\"Carl\",10000,2005-11-21,\"Controllers\",130000.00\n"
                        + "\"Diane\",10001,2012-09-20,\"\",130000.00\n"
                        + "\"Ed\",10002,,,0.00"
        );

        DataFrame loaded = dataSet.loadAsDataFrame();

        DataFrame expected = new DataFrame("Expected")
                .addStringColumn("Name").addLongColumn("EmployeeId").addDateColumn("HireDate").addStringColumn("Dept").addDoubleColumn("Salary")
                .addRow("Alice", 1234, LocalDate.of(2020, 1, 1), "Accounting", 110000.0)
                .addRow("Bob", 1233, LocalDate.of(2010, 1, 1), "Bee-bee-boo-boo", 100000.0)
                .addRow("Carl", 10000, LocalDate.of(2005, 11, 21), "Controllers", 130000.0)
                .addRow("Diane", 10001, LocalDate.of(2012, 9, 20), "", 130000.0)
                .addRow("Ed", 10002, null, "", 0.0);

        DataFrameUtil.assertEquals(expected, loaded);
    }

    @Test
    public void dateParsingWithSchemaFormat1()
    {
        CsvSchema schema = new CsvSchema();
        schema.addColumn("Name", ValueType.STRING);
        schema.addColumn("Date", ValueType.DATE, "d-MMM-uuuu");

        CsvDataSet dataSet = new StringBasedCsvDataSet("Foo", "Dates", schema,
                "Name,Date\n"
                        + "\"Alice\",1-Jan-2020\n"
                        + "\"Bob\",01-Jan-2010\n"
                        + "\"Carl\",21-Nov-2005\n"
                        + "\"Diane\",2-Sep-2012\n"
                        + "\"Ed\","
        );

        DataFrame loaded = dataSet.loadAsDataFrame();

        DataFrame expected = new DataFrame("Expected")
                .addStringColumn("Name").addDateColumn("Date")
                .addRow("Alice", LocalDate.of(2020, 1, 1))
                .addRow("Bob", LocalDate.of(2010, 1, 1))
                .addRow("Carl", LocalDate.of(2005, 11, 21))
                .addRow("Diane", LocalDate.of(2012, 9, 2))
                .addRow("Ed", null);

        DataFrameUtil.assertEquals(expected, loaded);
    }

    @Test
    public void dateParsingWithSchemaFormat2()
    {
        CsvSchema schema = new CsvSchema();
        schema.addColumn("Name", ValueType.STRING);
        schema.addColumn("Date", ValueType.DATE, "M/d/uuuu h:m:s a");

        CsvDataSet dataSet = new StringBasedCsvDataSet("Foo", "Dates", schema,
                "Name,Date\n"
                        + "\"Alice\",11/11/2020 11:11:11 AM\n"
                        + "\"Bob\",01/31/2010 12:12:00 PM\n"
                        + "\"Carl\",1/5/2005 12:00:00 AM\n"
                        + "\"Diane\",2/09/2012 4:55:15 PM\n"
                        + "\"Ed\",\n"
                        + "\"Frank\",2/19/2012 04:05:15 PM\n"
        );

        DataFrame loaded = dataSet.loadAsDataFrame();

        DataFrame expected = new DataFrame("Expected")
                .addStringColumn("Name").addDateColumn("Date")
                .addRow("Alice", LocalDate.of(2020, 11, 11))
                .addRow("Bob", LocalDate.of(2010, 1, 31))
                .addRow("Carl", LocalDate.of(2005, 1, 5))
                .addRow("Diane", LocalDate.of(2012, 2, 9))
                .addRow("Ed", null)
                .addRow("Frank", LocalDate.of(2012, 2, 19));

        DataFrameUtil.assertEquals(expected, loaded);
    }

    @Test(expected = RuntimeException.class)
    public void headerDataMismatchThrowsException()
    {
        CsvDataSet dataSet = new StringBasedCsvDataSet("Foo", "Dates",
                "Name,Date\n"
                        + "\"Alice\",1-Jan-2020,10\n"
                        + "\"Bob\",01-Jan-2010,11\n"
                        + "\"Carl\",21-Nov-2005,12\n"
                        + "\"Diane\",2-Sep-2012,13\n"
                        + "\"Ed\","
        );

        DataFrame loaded = dataSet.loadAsDataFrame();

        Assert.assertNotNull(loaded);
        Assert.fail("Didn't throw");
    }

    @Test(expected = RuntimeException.class)
    public void schemaDataMismatchThrowsException()
    {

        CsvSchema schema = new CsvSchema();
        schema.addColumn("Name", ValueType.STRING);
        schema.addColumn("Date", ValueType.DATE, "d-MMM-uuuu");
        schema.addColumn("Number", ValueType.LONG);

        CsvDataSet dataSet = new StringBasedCsvDataSet("Foo", "Dates", schema,
                "Name,Date\n"
                        + "\"Alice\",1-Jan-2020\n"
                        + "\"Bob\",01-Jan-2010\n"
                        + "\"Carl\",21-Nov-2005\n"
                        + "\"Diane\",2-Sep-2012\n"
                        + "\"Ed\","
        );

        DataFrame loaded = dataSet.loadAsDataFrame();

        Assert.assertNotNull(loaded);
        Assert.fail("Didn't throw");
    }

    @Test(expected = RuntimeException.class)
    public void schemaHeaderMismatchThrowsException()
    {

        CsvSchema schema = new CsvSchema();
        schema.addColumn("Name", ValueType.STRING);
        schema.addColumn("Date", ValueType.DATE, "d-MMM-uuuu");

        CsvDataSet dataSet = new StringBasedCsvDataSet("Foo", "Dates", schema,
                "Name,Date,Number\n"
                        + "\"Alice\",1-Jan-2020\n"
                        + "\"Bob\",01-Jan-2010\n"
                        + "\"Carl\",21-Nov-2005\n"
                        + "\"Diane\",2-Sep-2012\n"
                        + "\"Ed\","
        );

        DataFrame loaded = dataSet.loadAsDataFrame();

        Assert.assertNotNull(loaded);
        Assert.fail("Didn't throw");
    }

    @Test
    public void loadWithSchema()
    {
        CsvSchema schema = new CsvSchema()
                .separator('|')
                .quoteCharacter('\'')
                .nullMarker("-NULL-");
        schema.addColumn("Name", ValueType.STRING);
        schema.addColumn("EmployeeId", ValueType.LONG);
        schema.addColumn("HireDate", ValueType.DATE);
        schema.addColumn("Salary", ValueType.DOUBLE);

        CsvDataSet dataSet = new StringBasedCsvDataSet("Foo", "Employees", schema,
                "Name|EmployeeId|HireDate|Salary\n"
                        + "'Alice'|1234|2020-01-01|110000.00\n"
                        + "'Bob'|1233|2010-01-01|-NULL-\n"
                        + "'Carl'||2005-11-21|130000.00\n"
                        + "'Diane'|10001|2012-09-20|130000.00\n"
                        + "'Ed'|10002|-NULL-|0.00"
        );

        DataFrame loaded = dataSet.loadAsDataFrame();

        DataFrame expected = new DataFrame("Expected")
                .addStringColumn("Name").addLongColumn("EmployeeId").addDateColumn("HireDate").addDoubleColumn("Salary")
                .addRow("Alice", 1234, LocalDate.of(2020, 1, 1), 110000.0)
                .addRow("Bob", 1233, LocalDate.of(2010, 1, 1), 0.0)
                .addRow("Carl", 0, LocalDate.of(2005, 11, 21), 130000.0)
                .addRow("Diane", 10001, LocalDate.of(2012, 9, 20), 130000.0)
                .addRow("Ed", 10002, null, 0.0);

        DataFrameUtil.assertEquals(expected, loaded);
    }

    @Test
    public void headFewerThanTotalLines()
    {
        CsvDataSet dataSet = new StringBasedCsvDataSet("Foo", "Employees",
                "Name,EmployeeId,HireDate,Dept,Salary\n"
                        + "\"Alice\",1234,2020-01-01,\"Accounting\",110000.00\n"
                        + "\"Bob\",1233,2010-01-01,\"Bee-bee-boo-boo\",100000.00\n"
                        + "\"Carl\",10000,2005-11-21,\"Controllers\",130000.00\n"
                        + "\"Diane\",10001,2012-09-20,\"\",130000.00\n"
                        + "\"Ed\",10002,,,0.00"
        );

        DataFrame loaded = dataSet.loadAsDataFrame(2);

        DataFrame expected = new DataFrame("Expected")
                .addStringColumn("Name").addLongColumn("EmployeeId").addDateColumn("HireDate").addStringColumn("Dept").addDoubleColumn("Salary")
                .addRow("Alice", 1234, LocalDate.of(2020, 1, 1), "Accounting", 110000.0)
                .addRow("Bob", 1233, LocalDate.of(2010, 1, 1), "Bee-bee-boo-boo", 100000.0);

        DataFrameUtil.assertEquals(expected, loaded);
    }

    @Test
    public void headMoreThanTotalLines()
    {
        CsvDataSet dataSet = new StringBasedCsvDataSet("Foo", "Employees",
                "Name,EmployeeId,HireDate,Dept,Salary\n"
                        + "\"Alice\",1234,2020-01-01,\"Accounting\",110000.00\n"
                        + "\"Bob\",1233,2010-01-01,\"Bee-bee-boo-boo\",100000.00\n"
                        + "\"Carl\",10000,2005-11-21,\"Controllers\",130000.00\n"
                        + "\"Diane\",10001,2012-09-20,\"\",130000.00\n"
                        + "\"Ed\",10002,,,0.00"
        );

        DataFrame loaded = dataSet.loadAsDataFrame(100);

        DataFrame expected = new DataFrame("Expected")
                .addStringColumn("Name").addLongColumn("EmployeeId").addDateColumn("HireDate").addStringColumn("Dept").addDoubleColumn("Salary")
                .addRow("Alice", 1234, LocalDate.of(2020, 1, 1), "Accounting", 110000.0)
                .addRow("Bob", 1233, LocalDate.of(2010, 1, 1), "Bee-bee-boo-boo", 100000.0)
                .addRow("Carl", 10000, LocalDate.of(2005, 11, 21), "Controllers", 130000.0)
                .addRow("Diane", 10001, LocalDate.of(2012, 9, 20), "", 130000.0)
                .addRow("Ed", 10002, null, "", 0.0);

        DataFrameUtil.assertEquals(expected, loaded);
    }

    @Test
    public void headZeroOrNegativeLines()
    {
        CsvDataSet dataSet = new StringBasedCsvDataSet("Foo", "Employees",
                "Name,EmployeeId,HireDate,Dept,Salary\n"
                        + "\"Alice\",1234,2020-01-01,\"Accounting\",110000.00\n"
                        + "\"Bob\",1233,2010-01-01,\"Bee-bee-boo-boo\",100000.00\n"
                        + "\"Carl\",10000,2005-11-21,\"Controllers\",130000.00\n"
                        + "\"Diane\",10001,2012-09-20,\"\",130000.00\n"
                        + "\"Ed\",10002,,,0.00"
        );

        DataFrame loaded = dataSet.loadAsDataFrame(0);

        Assert.assertNotNull(loaded);
        Assert.assertEquals(0, loaded.rowCount());

        loaded = dataSet.loadAsDataFrame(-1);

        Assert.assertNotNull(loaded);
        Assert.assertEquals(0, loaded.rowCount());
    }

    @Test
    public void loadEmptyNoSchema()
    {
        CsvDataSet dataSet = new StringBasedCsvDataSet("Foo", "Dates",
                "Name,Date,Number\n"
        );

        DataFrame loaded = dataSet.loadAsDataFrame();

        Assert.assertEquals(0, loaded.rowCount());
        Assert.assertEquals(3, loaded.columnCount());
    }

    @Test
    public void loadEmptyWithSchema()
    {
        CsvSchema schema = new CsvSchema();
        schema.addColumn("Name", ValueType.STRING);
        schema.addColumn("Date", ValueType.DATE, "d-MMM-uuuu");
        schema.addColumn("Number", ValueType.LONG);

        CsvDataSet dataSet = new StringBasedCsvDataSet("Foo", "Dates", schema,
                "Name,Date,Number\n"
        );

        DataFrame loaded = dataSet.loadAsDataFrame();

        Assert.assertEquals(0, loaded.rowCount());
        Assert.assertEquals(3, loaded.columnCount());
    }

    @Test
    public void headersEnclosedInQuotes()
    {
        CsvDataSet dataSet = new StringBasedCsvDataSet("Foo", "Employees",
                "\"Name\",\"Employee Id\",\"Hire Date\",Dept,\"Salary, USD\"\n"
                        + "\"Alice\",1234,2020-01-01,\"Accounting\",110000.00\n"
                        + "\"Bob\",1233,2010-01-01,\"Bee-bee-boo-boo\",100000.00\n"
        );

        DataFrame loaded = dataSet.loadAsDataFrame();

        DataFrameUtil.assertEquals(
                new DataFrame("expected")
                .addStringColumn("Name").addLongColumn("Employee Id").addDateColumn("Hire Date")
                .addStringColumn("Dept").addDoubleColumn("Salary, USD")
                .addRow("Alice", 1234, LocalDate.of(2020, 1, 1), "Accounting", 110000.0)
                .addRow("Bob", 1233, LocalDate.of(2010, 1, 1), "Bee-bee-boo-boo", 100000.0)
                , loaded);
    }

    @Test
    public void headersWithSpaces()
    {
        CsvDataSet dataSet = new StringBasedCsvDataSet("Foo", "Employees",
                "Name,Employee Id,Hire Date,Dept,Salary in USD\n"
                        + "\"Alice\",1234,2020-01-01,\"Accounting\",110000.00\n"
                        + "\"Bob\",1233,2010-01-01,\"Bee-bee-boo-boo\",100000.00\n"
        );

        DataFrame loaded = dataSet.loadAsDataFrame();

        DataFrameUtil.assertEquals(
                new DataFrame("expected")
                .addStringColumn("Name").addLongColumn("Employee Id").addDateColumn("Hire Date")
                .addStringColumn("Dept").addDoubleColumn("Salary in USD")
                .addRow("Alice", 1234, LocalDate.of(2020, 1, 1), "Accounting", 110000.0)
                .addRow("Bob", 1233, LocalDate.of(2010, 1, 1), "Bee-bee-boo-boo", 100000.0)
                , loaded);
    }

    @Test
    public void columnTypeInference()
    {
        CsvDataSet dataSet = new StringBasedCsvDataSet("Foo", "Employees",
                  "Name,EmployeeId,DeptNo,HireDate,Salary,MaybeNumber\n"
                + "\"Alice\",1234,,2020-01-01,110000,\n"
                + "\"Bob\",1235,100,2010-01-01,100000.50,12\n"
                + "\"Carl\",1236,100,Wednesday,100000.60,12.34\n"
                + "\"Doris\",1237,101,2010-01-01,100000.70,Hi\n"
        );

        DataFrame loaded = dataSet.loadAsDataFrame();

        CsvSchema schema = dataSet.getSchema();

        MutableMap<String, ValueType> typeByName =
                schema.getColumns().toMap(CsvSchemaColumn::getName, CsvSchemaColumn::getType);

        Assert.assertEquals(ValueType.STRING, typeByName.get("Name"));
        Assert.assertEquals(ValueType.LONG,   typeByName.get("EmployeeId"));
        Assert.assertEquals(ValueType.LONG,   typeByName.get("DeptNo"));
        Assert.assertEquals(ValueType.STRING, typeByName.get("HireDate"));
        Assert.assertEquals(ValueType.DOUBLE, typeByName.get("Salary"));
        Assert.assertEquals(ValueType.STRING, typeByName.get("MaybeNumber"));
    }

    @Test
    public void columnTypeInferenceWithMoreEmptyValues()
    {
        CsvDataSet dataSet = new StringBasedCsvDataSet("Foo", "Employees",
                  "Name,EmployeeId,DeptNo,HireDate,OtherDate,Salary,MaybeNumber\n"
                + ",1234,,2020-01-01,2020-01-01,110000,\n"
                + "\"Bob\",1235,100,2010-01-01,2010-01-01,100000.50,12\n"
                + ",1236,,10/25/2015,2015-10-25,,12.34\n"
                + "\"Doris\",,101,2010-01-01,2010-01-01,100000.70,Hi\n"
        );

        DataFrame loaded = dataSet.loadAsDataFrame();

        CsvSchema schema = dataSet.getSchema();

        MutableMap<String, ValueType> typeByName =
                schema.getColumns().toMap(CsvSchemaColumn::getName, CsvSchemaColumn::getType);

        Assert.assertEquals(ValueType.STRING, typeByName.get("Name"));
        Assert.assertEquals(ValueType.LONG,   typeByName.get("EmployeeId"));
        Assert.assertEquals(ValueType.LONG,   typeByName.get("DeptNo"));
        Assert.assertEquals(ValueType.STRING, typeByName.get("HireDate"));
        Assert.assertEquals(ValueType.DATE,   typeByName.get("OtherDate"));
        Assert.assertEquals(ValueType.DOUBLE, typeByName.get("Salary"));
        Assert.assertEquals(ValueType.STRING, typeByName.get("MaybeNumber"));
    }
}
