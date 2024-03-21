package io.github.vmzakharov.ecdataframe.dataset;

import io.github.vmzakharov.ecdataframe.dataframe.DataFrame;
import io.github.vmzakharov.ecdataframe.dataframe.DataFrameUtil;
import io.github.vmzakharov.ecdataframe.dsl.value.ValueType;
import org.eclipse.collections.api.map.MutableMap;
import org.junit.Assert;
import org.junit.Test;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;

import static io.github.vmzakharov.ecdataframe.dsl.value.ValueType.DATE;
import static io.github.vmzakharov.ecdataframe.dsl.value.ValueType.DATE_TIME;
import static io.github.vmzakharov.ecdataframe.dsl.value.ValueType.DECIMAL;
import static io.github.vmzakharov.ecdataframe.dsl.value.ValueType.DOUBLE;
import static io.github.vmzakharov.ecdataframe.dsl.value.ValueType.INT;
import static io.github.vmzakharov.ecdataframe.dsl.value.ValueType.LONG;
import static io.github.vmzakharov.ecdataframe.dsl.value.ValueType.STRING;

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
    public void dateTimeDefaultParsing()
    {
        CsvDataSet dataSet = new StringBasedCsvDataSet("Foo", "Dates And Also Times",
                "Name,DateTime\n"
                + "\"Alice\",2020-9-22T13:14:15\n"
                + "\"Bob\",2021-10-23T13:14:16\n"
                + "\"Carl\",\n"
                + "\"Diane\",2023-12-24T13:14:15\n"
        );

        DataFrame loaded = dataSet.loadAsDataFrame();

        DataFrame expected = new DataFrame("Expected")
                .addStringColumn("Name").addDateTimeColumn("DateTime")
                .addRow("Alice", LocalDateTime.of(2020,  9, 22, 13, 14, 15))
                .addRow("Bob",   LocalDateTime.of(2021, 10, 23, 13, 14, 16))
                .addRow("Carl", null)
                .addRow("Diane", LocalDateTime.of(2023, 12, 24, 13, 14, 15)
            );

        DataFrameUtil.assertEquals(expected, loaded);
    }

    @Test
    public void dateTimeWithSchema()
    {
        CsvSchema schema = new CsvSchema();
        schema.addColumn("Name", STRING);
        schema.addColumn("DateTime", DATE_TIME, "uuuu*M*d-HH*mm*ss");

        CsvDataSet dataSet = new StringBasedCsvDataSet("Foo", "Dates And Also Times", schema,
                "Name,DateTime\n"
                + "\"Alice\",2020*9*22-13*14*15\n"
                + "\"Bob\",  2021*10*23-13*14*16\n"
                + "\"Carl\", \n"
                + "\"Diane\",2023*12*24-13*15*15\n"
        );

        DataFrame loaded = dataSet.loadAsDataFrame();

        DataFrame expected = new DataFrame("Expected")
                .addStringColumn("Name").addDateTimeColumn("DateTime")
                .addRow("Alice", LocalDateTime.of(2020,  9, 22, 13, 14, 15))
                .addRow("Bob",   LocalDateTime.of(2021, 10, 23, 13, 14, 16))
                .addRow("Carl", null)
                .addRow("Diane", LocalDateTime.of(2023, 12, 24, 13, 15, 15)
            );

        DataFrameUtil.assertEquals(expected, loaded);
    }

    @Test
    public void loadDataWithSchemaNullsConvertedToValues()
    {
        CsvSchema schema = new CsvSchema()
                .addColumn("Name", STRING)
                .addColumn("EmployeeId", LONG)
                .addColumn("HireDate", DATE, "uuuu-M-d")
                .addColumn("Dept", STRING)
                .addColumn("Salary", DOUBLE)
                .addColumn("PetCount", INT);

        CsvDataSet dataSet = new StringBasedCsvDataSet("Foo", "Employees", schema,
                "Name,EmployeeId,HireDate,Dept,Salary,PetCount\n"
                        + "\"Alice\",1234,2020-01-01,\"Accounting\",110000.00,2\n"
                        + "\"Bob\",1233,2010-01-01,\"Bee-bee-boo-boo\",100000.00,0\n"
                        + "\"Carl\",10000,2005-11-21,\"Controllers\",130000.00,1\n"
                        + "\"Diane\",10001,2012-09-20,\"\",130000.00,8\n"
                        + "\"Ed\",10002,,,0.00,"
        );

        DataFrame loaded = dataSet.loadAsDataFrame();

        DataFrame expected = new DataFrame("Expected")
                .addStringColumn("Name").addLongColumn("EmployeeId").addDateColumn("HireDate").addStringColumn("Dept")
                .addDoubleColumn("Salary").addIntColumn("PetCount")
                .addRow("Alice", 1234, LocalDate.of(2020, 1, 1), "Accounting", 110000.0, 2)
                .addRow("Bob", 1233, LocalDate.of(2010, 1, 1), "Bee-bee-boo-boo", 100000.0, 0)
                .addRow("Carl", 10000, LocalDate.of(2005, 11, 21), "Controllers", 130000.0, 1)
                .addRow("Diane", 10001, LocalDate.of(2012, 9, 20), "", 130000.0, 8)
                .addRow("Ed", 10002, null, "", 0.0, 0);

        DataFrameUtil.assertEquals(expected, loaded);
    }

    @Test
    public void loadDataWithSchemaIntId()
    {
        CsvSchema schema = new CsvSchema();
        schema.addColumn("Name", STRING);
        schema.addColumn("EmployeeId", INT);
        schema.addColumn("HireDate", DATE, "uuuu-M-d");
        schema.addColumn("Dept", STRING);
        schema.addColumn("Salary", DOUBLE);

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
                .addStringColumn("Name").addIntColumn("EmployeeId").addDateColumn("HireDate").addStringColumn("Dept").addDoubleColumn("Salary")
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
        schema.addColumn("Name", STRING);
        schema.addColumn("Date", DATE, "d-MMM-uuuu");

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
        schema.addColumn("Name", STRING);
        schema.addColumn("Date", DATE, "M/d/uuuu h:m:s a");

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
                "Name,Date,Count\n"
                + "\"Alice\",1-Jan-2020,10\n"
                + "\"Bob\",01-Jan-2010,11\n"
                + "\"Carl\",21-Nov-2005,12\n"
                + "\"Diane\",2-Sep-2012,13\n"
                + "\"Ed\","
        );

        DataFrame loaded = dataSet.loadAsDataFrame();

        Assert.assertNotNull(loaded);
    }

    @Test
    public void headerDataMismatchThrowsExceptionWithCorrectLineNumber()
    {
        try
        {
            CsvDataSet dataSet = new StringBasedCsvDataSet("Foo", "Dates",
                    "Name,Date,Count\n"
                    + "\"Alice\",1-Jan-2020,10\n"
                    + "\"Bob\",01-Jan-2010,11\n"
                    + "\"Carl\",21-Nov-2005,12\n"
                    + "\"Diane\",2-Sep-2012,13\n"
                    + "\"Ed\","
            );

            DataFrame loaded = dataSet.loadAsDataFrame();
        }
        catch (RuntimeException re)
        {
            String expectedMessage = "The number of elements in the header does not match the number of elements in the data row 5 (3 vs 2)";
            Assert.assertEquals(expectedMessage, re.getMessage());
        }
    }

    @Test(expected = RuntimeException.class)
    public void schemaColumnCountMismatchThrowsException()
    {

        CsvSchema schema = new CsvSchema();
        schema.addColumn("Name", STRING);
        schema.addColumn("Date", DATE, "d-MMM-uuuu");
        schema.addColumn("Number", LONG);

        CsvDataSet dataSet = new StringBasedCsvDataSet("Foo", "Dates", schema,
                "Name,Date\n"
                + "\"Alice\",1-Jan-2020\n"
                + "\"Bob\",01-Jan-2010\n"
                + "\"Carl\",21-Nov-2005\n"
                + "\"Diane\",2-Sep-2012\n"
                + "\"Ed\","
        );

        Assert.assertNotNull(dataSet.loadAsDataFrame());
    }

    @Test(expected = RuntimeException.class)
    public void schemaColumnNameMismatchThrowsException()
    {

        CsvSchema schema = new CsvSchema();
        schema.addColumn("Name", STRING);
        schema.addColumn("Date", DATE, "d-MMM-uuuu");
        schema.addColumn("Number", LONG);

        CsvDataSet dataSet = new StringBasedCsvDataSet("Foo", "Dates", schema,
                "Name,Something,Number\n"
                + "\"Alice\",1-Jan-2020,1\n"
                + "\"Bob\",01-Jan-2010,2\n"
                + "\"Carl\",21-Nov-2005,3\n"
                + "\"Diane\",2-Sep-2012,4\n"
                + "\"Ed\",,5"
        );

        Assert.assertNotNull(dataSet.loadAsDataFrame());
    }

    @Test(expected = RuntimeException.class)
    public void schemaHeaderMismatchThrowsException()
    {
        CsvSchema schema = new CsvSchema();
        schema.addColumn("Name", STRING);
        schema.addColumn("Date", DATE, "d-MMM-uuuu");

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
    }

    @Test
    public void loadWithSchema()
    {
        CsvSchema schema = new CsvSchema()
                .separator('|')
                .quoteCharacter('\'')
                .nullMarker("-NULL-");
        schema.addColumn("Name", STRING);
        schema.addColumn("EmployeeId", LONG);
        schema.addColumn("HireDate", DATE);
        schema.addColumn("Salary", DOUBLE);

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
                .addRow("Bob", 1233, LocalDate.of(2010, 1, 1), null)
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
        schema.addColumn("Name", STRING);
        schema.addColumn("Date", DATE, "d-MMM-uuuu");
        schema.addColumn("Number", LONG);

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
                "Name,\"Employee Id\",Hire Date,Dept,Salary in USD\n"
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
    public void inferSchema()
    {
        CsvDataSet dataSet = new StringBasedCsvDataSet("Foo", "Employees",
                  "Name,EmployeeId,DeptNo,HireDate,Salary,MaybeNumber\n"
                + "\"Alice\",1234,,2020-01-01,110000,\n"
                + "\"Bob\",1235,100,2010-01-01,100000.50,12\n"
                + "\"Carl\",1236,100,Wednesday,100000.60,12.34\n"
                + "\"Doris\",1237,101,2010-01-01,100000.70,Hi\n"
        );

        CsvSchema schema = dataSet.inferSchema();

        MutableMap<String, ValueType> typeByName =
                schema.getColumns().toMap(CsvSchemaColumn::getName, CsvSchemaColumn::getType);

        Assert.assertEquals(STRING, typeByName.get("Name"));
        Assert.assertEquals(LONG,   typeByName.get("EmployeeId"));
        Assert.assertEquals(LONG,   typeByName.get("DeptNo"));
        Assert.assertEquals(STRING, typeByName.get("HireDate"));
        Assert.assertEquals(DOUBLE, typeByName.get("Salary"));
        Assert.assertEquals(STRING, typeByName.get("MaybeNumber"));
    }

    @Test
    public void inferSchemaFromFirstTwoLines()
    {
        CsvDataSet dataSet = new StringBasedCsvDataSet("Foo", "Employees",
                  "Name,EmployeeId,DeptNo,HireDate,Salary,MaybeNumber\n"
                  + "\"Alice\",1234,,2020-01-01,110000,\n"
                  + "\"Bob\",1235,100,2010-01-01,100000.50,12\n"
                  + "\"Carl\",1236,100,Wednesday,100000.60,12.34\n"
                  + "\"Doris\",1237,101,2010-01-01,100000.70,Hi\n"
        );

        CsvSchema schema = dataSet.inferSchema(2);

        MutableMap<String, ValueType> typeByName =
                schema.getColumns().toMap(CsvSchemaColumn::getName, CsvSchemaColumn::getType);

        Assert.assertEquals(STRING, typeByName.get("Name"));
        Assert.assertEquals(LONG,   typeByName.get("EmployeeId"));
        Assert.assertEquals(LONG,   typeByName.get("DeptNo"));
        Assert.assertEquals(DATE,   typeByName.get("HireDate"));
        Assert.assertEquals(DOUBLE, typeByName.get("Salary"));
        Assert.assertEquals(LONG,   typeByName.get("MaybeNumber"));
    }

    @Test
    public void inferSchemaFromOneDataLine()
    {
        CsvDataSet dataSet = new StringBasedCsvDataSet("Foo", "Employees",
                "Name,EmployeeId,DeptNo,HireDate,Salary,MaybeNumber\n"
                        + "\"Alice\",1234,,2020-01-01,110000.12,Hi\n"
        );

        CsvSchema schema = dataSet.inferSchema();

        MutableMap<String, ValueType> typeByName =
                schema.getColumns().toMap(CsvSchemaColumn::getName, CsvSchemaColumn::getType);

        Assert.assertEquals(STRING, typeByName.get("Name"));
        Assert.assertEquals(LONG,   typeByName.get("EmployeeId"));
        Assert.assertEquals(STRING, typeByName.get("DeptNo"));
        Assert.assertEquals(DATE,   typeByName.get("HireDate"));
        Assert.assertEquals(DOUBLE, typeByName.get("Salary"));
        Assert.assertEquals(STRING, typeByName.get("MaybeNumber"));
    }

    @Test
    public void inferSchemaFromNoDataLines()
    {
        CsvDataSet dataSet = new StringBasedCsvDataSet("Foo", "Employees",
                "Name,EmployeeId,DeptNo,HireDate,Salary,MaybeNumber\n"
        );

        CsvSchema schema = dataSet.inferSchema();

        MutableMap<String, ValueType> typeByName =
                schema.getColumns().toMap(CsvSchemaColumn::getName, CsvSchemaColumn::getType);

        Assert.assertEquals(STRING, typeByName.get("Name"));
        Assert.assertEquals(STRING, typeByName.get("EmployeeId"));
        Assert.assertEquals(STRING, typeByName.get("DeptNo"));
        Assert.assertEquals(STRING, typeByName.get("HireDate"));
        Assert.assertEquals(STRING, typeByName.get("Salary"));
        Assert.assertEquals(STRING, typeByName.get("MaybeNumber"));
    }

    @Test
    public void inferSchemaMakeSureTheFirstLineProcessed()
    {
        CsvDataSet dataSet = new StringBasedCsvDataSet("Foo", "Employees",
                "Name,EmployeeId,DeptNo,HireDate,Salary,MaybeNumber\n"
                        + "\"Alice\",One,Two,Three,Four,Five\n"
                        + "\"Bob\",1235,100,2010-01-01,100000.50,12\n"
                        + "\"Doris\",1237,101,2010-01-01,100000.70,15\n"
        );

        CsvSchema schema = dataSet.inferSchema();

        MutableMap<String, ValueType> typeByName =
                schema.getColumns().toMap(CsvSchemaColumn::getName, CsvSchemaColumn::getType);

        Assert.assertEquals(STRING, typeByName.get("Name"));
        Assert.assertEquals(STRING, typeByName.get("EmployeeId"));
        Assert.assertEquals(STRING, typeByName.get("DeptNo"));
        Assert.assertEquals(STRING, typeByName.get("HireDate"));
        Assert.assertEquals(STRING, typeByName.get("Salary"));
        Assert.assertEquals(STRING, typeByName.get("MaybeNumber"));
    }

    @Test(expected = RuntimeException.class)
    public void inferSchemaWithMismatchedRowsFails()
    {
        CsvDataSet dataSet = new StringBasedCsvDataSet("Foo", "Employees",
                "Name,EmployeeId,DeptNo,HireDate,Salary,MaybeNumber\n"
                        + "\"Alice\",1234,,110000,\n"
                        + "\"Bob\",1235,100,2010-01-01,100000.50,12\n"
        );

        CsvSchema schema = dataSet.inferSchema();
        Assert.assertNotNull(schema);
    }

    @Test
    public void inferSchemaOnLoad()
    {
        CsvDataSet dataSet = new StringBasedCsvDataSet("Foo", "Employees",
                "Name,EmployeeId,DeptNo,HireDate,Salary,MaybeNumber\n"
                        + "\"Alice\",1234,,2020-01-01,110000,\n"
                        + "\"Bob\",1235,100,2010-01-01,100000.50,12\n"
                        + "\"Carl\",1236,100,Wednesday,100000.60,12.34\n"
                        + "\"Doris\",1237,101,2010-01-01,100000.70,Hi\n"
        );

        DataFrame loaded = dataSet.loadAsDataFrame();

        DataFrame expected = new DataFrame("expected")
                .addStringColumn("Name").addLongColumn("EmployeeId").addLongColumn("DeptNo").addStringColumn("HireDate")
                .addDoubleColumn("Salary").addStringColumn("MaybeNumber")
                .addRow("Alice", 1234,   0, "2020-01-01", 110000.00, "")
                .addRow("Bob",   1235, 100, "2010-01-01", 100000.50, "12")
                .addRow("Carl",  1236, 100, "Wednesday",  100000.60, "12.34")
                .addRow("Doris", 1237, 101, "2010-01-01", 100000.70, "Hi")
                ;

        DataFrameUtil.assertEquals(expected, loaded);
    }

    @Test
    public void inferSchemaAndThenLoad()
    {
        CsvDataSet dataSet = new StringBasedCsvDataSet("Foo", "Employees",
                "Name,EmployeeId,DeptNo,HireDate,Salary,MaybeNumber\n"
                        + "\"Alice\",1234,,2020-01-01,110000,\n"
                        + "\"Bob\",1235,100,2010-01-01,100000.50,12\n"
                        + "\"Carl\",1236,100,Wednesday,100000.60,12.34\n"
                        + "\"Doris\",1237,101,2010-01-01,100000.70,Hi\n"
        );

        dataSet.inferSchema();

        DataFrame loaded = dataSet.loadAsDataFrame();

        DataFrame expected = new DataFrame("expected")
                .addStringColumn("Name").addLongColumn("EmployeeId").addLongColumn("DeptNo").addStringColumn("HireDate")
                .addDoubleColumn("Salary").addStringColumn("MaybeNumber")
                .addRow("Alice", 1234,   0, "2020-01-01", 110000.00, "")
                .addRow("Bob",   1235, 100, "2010-01-01", 100000.50, "12")
                .addRow("Carl",  1236, 100, "Wednesday",  100000.60, "12.34")
                .addRow("Doris", 1237, 101, "2010-01-01", 100000.70, "Hi")
                ;

        DataFrameUtil.assertEquals(expected, loaded);
    }

    @Test
    public void inferSchemaWithMoreEmptyValues()
    {
        CsvDataSet dataSet = new StringBasedCsvDataSet("Foo", "Employees",
                  "Name,EmployeeId,DeptNo,HireDate,OtherDate,Salary,All Empty,MaybeNumber\n"
                + ",1234,,2020-01-01,2020-01-01,110000,,\n"
                + "\"Bob\",1235,100,2010-01-01,2010-01-01,100000.50,,12\n"
                + ",1236,,10/25/2015,2015-10-25,,,12.34\n"
                + "\"Doris\",,101,2010-01-01,2010-01-01,100000.70,,Hi\n"
        );

        CsvSchema schema = dataSet.inferSchema();

        MutableMap<String, ValueType> typeByName =
                schema.getColumns().toMap(CsvSchemaColumn::getName, CsvSchemaColumn::getType);

        Assert.assertEquals(STRING, typeByName.get("Name"));
        Assert.assertEquals(LONG,   typeByName.get("EmployeeId"));
        Assert.assertEquals(LONG,   typeByName.get("DeptNo"));
        Assert.assertEquals(STRING, typeByName.get("HireDate"));
        Assert.assertEquals(DATE,   typeByName.get("OtherDate"));
        Assert.assertEquals(DOUBLE, typeByName.get("Salary"));
        Assert.assertEquals(STRING, typeByName.get("All Empty"));
        Assert.assertEquals(STRING, typeByName.get("MaybeNumber"));
    }

    @Test
    public void loadWithNulls()
    {
        CsvSchema schema = new CsvSchema()
            .addColumn("Name", STRING)
            .addColumn("EmployeeId", LONG)
            .addColumn("HireDate", DATE, "uuuu-M-d")
            .addColumn("Dept", STRING)
            .addColumn("Salary", DOUBLE)
            .addColumn("Abc", STRING)
            .addColumn("PetCount", INT);

        CsvDataSet dataSet = new StringBasedCsvDataSet("Foo", "Employees", schema,
                "Name,EmployeeId,HireDate,Dept,Salary,Abc,PetCount\n"
                        + "\"Diane\",10001,2012-09-20,\"\",,\"ABC\",8\n"
                        + "\"Alice\",1234,2020-01-01,\"Accounting\",110000.00,,0\n"
                        + "\"Bob\",1233,2010-01-01,\"Bee-bee-boo-boo\",100000.00,\"ABC\",1\n"
                        + "\"Carl\",,2005-11-21,\"Controllers\",130000.00,\"ABC\",\n"
                        + "\"Ed\",10002,,,0.00,,2"
        );

        dataSet.convertEmptyElementsToNulls();

        DataFrame loaded = dataSet.loadAsDataFrame();

        DataFrame expected = new DataFrame("Expected")
                .addStringColumn("Name").addLongColumn("EmployeeId").addDateColumn("HireDate").addStringColumn("Dept")
                .addDoubleColumn("Salary").addStringColumn("Abc").addIntColumn("PetCount")
                .addRow("Diane", 10001, LocalDate.of(2012, 9, 20), "", null, "ABC", 8)
                .addRow("Alice", 1234, LocalDate.of(2020, 1, 1), "Accounting", 110000.0, null, 0)
                .addRow("Bob", 1233, LocalDate.of(2010, 1, 1), "Bee-bee-boo-boo", 100000.0, "ABC", 1)
                .addRow("Carl", null, LocalDate.of(2005, 11, 21), "Controllers", 130000.0, "ABC", null)
                .addRow("Ed", 10002, null, null, 0.0, null, 2);

        DataFrameUtil.assertEquals(expected, loaded);
    }

    @Test
    public void inferSchemaThenLoadWithNulls()
    {
        String data = "Name,EmployeeId,HireDate,Dept,Salary,Abc\n"
                + "\"Diane\",10001,2012-09-20,\"\",,\"ABC\"\n"
                + "\"Alice\",1234,2020-01-01,\"Accounting\",110000.00,\n"
                + "\"Bob\",1233,2010-01-01,\"Bee-bee-boo-boo\",100000.00,\"ABC\"\n"
                + "\"Carl\",,2005-11-21,\"Controllers\",130000.00,\"ABC\"\n"
                + "\"Ed\",10002,,,0.00,";

        CsvDataSet dataSetToInferFrom = new StringBasedCsvDataSet("Foo", "Employees", data);
        CsvSchema inferredSchema = dataSetToInferFrom.inferSchema();

        CsvDataSet dataSetToLoad = new StringBasedCsvDataSet("Foo", "Employees", inferredSchema, data);

        DataFrameUtil.assertEquals(new DataFrame("Expected")
                        .addStringColumn("Name").addLongColumn("EmployeeId").addDateColumn("HireDate").addStringColumn("Dept")
                        .addDoubleColumn("Salary").addStringColumn("Abc")
                        .addRow("Diane", 10001, LocalDate.of(2012, 9, 20), "", null, "ABC")
                        .addRow("Alice", 1234, LocalDate.of(2020, 1, 1), "Accounting", 110000.0, null)
                        .addRow("Bob", 1233, LocalDate.of(2010, 1, 1), "Bee-bee-boo-boo", 100000.0, "ABC")
                        .addRow("Carl", null, LocalDate.of(2005, 11, 21), "Controllers", 130000.0, "ABC")
                        .addRow("Ed", 10002, null, null, 0.0, null),
                dataSetToLoad
                        .convertEmptyElementsToNulls()
                        .loadAsDataFrame());

        DataFrameUtil.assertEquals(new DataFrame("Expected")
                        .addStringColumn("Name").addLongColumn("EmployeeId").addDateColumn("HireDate").addStringColumn("Dept")
                        .addDoubleColumn("Salary").addStringColumn("Abc")
                        .addRow("Diane", 10001, LocalDate.of(2012, 9, 20), "", 0.0, "ABC")
                        .addRow("Alice", 1234, LocalDate.of(2020, 1, 1), "Accounting", 110000.0, "")
                        .addRow("Bob", 1233, LocalDate.of(2010, 1, 1), "Bee-bee-boo-boo", 100000.0, "ABC")
                        .addRow("Carl", 0, LocalDate.of(2005, 11, 21), "Controllers", 130000.0, "ABC")
                        .addRow("Ed", 10002, null, "", 0.0, ""),
                dataSetToLoad
                        .convertEmptyElementsToValues()
                        .loadAsDataFrame());
    }

    @Test(expected = RuntimeException.class)
    public void emptyHeaderThrowsException()
    {
        CsvDataSet dataSet = new StringBasedCsvDataSet("Foo", "Employees",
                "Name,EmployeeId,,HireDate,Salary,MaybeNumber\n"
                        + "\"Bob\",1235,100,2010-01-01,100000.50,12\n"
                        + "\"Doris\",1237,101,2010-01-01,100000.70,15\n"
        );

        DataFrame df = dataSet.loadAsDataFrame();
    }

    @Test
    public void noHeaderLineWithSchema()
    {
        CsvSchema schema = new CsvSchema()
            .addColumn("Name", STRING)
            .addColumn("EmployeeId", LONG)
            .addColumn("HireDate", DATE, "uuuu-M-d")
            .addColumn("Salary", DOUBLE)
            .addColumn("DeptCode", LONG)
            .hasHeaderLine(false);

        CsvDataSet dataSet = new StringBasedCsvDataSet("Foo", "Employees", schema,
                  "\"Bob\",1235,2010-01-01,100000.50,12\n"
                + "\"Doris\",1237,2010-01-01,100000.70,15\n"
        );

        DataFrame df = dataSet.loadAsDataFrame();

        DataFrameUtil.assertEquals(
                new DataFrame("Employees")
                        .addStringColumn("Name").addLongColumn("EmployeeId").addDateColumn("HireDate").addDoubleColumn("Salary").addLongColumn("DeptCode")
                        .addRow("Bob", 1235, LocalDate.of(2010, 1, 1), 100_000.50, 12)
                        .addRow("Doris", 1237, LocalDate.of(2010, 1, 1), 100_000.70, 15),
                df
        );
    }

    @Test(expected = RuntimeException.class)
    public void duplicateHeader()
    {
        CsvDataSet dataSet = new StringBasedCsvDataSet("Foo", "Employees",
                "Name,EmployeeId,EmployeeId,HireDate,Salary,MaybeNumber\n"
                        + "\"Bob\",1235,100,2010-01-01,100000.50,12\n"
                        + "\"Doris\",1237,101,2010-01-01,100000.70,15\n"
        );

        DataFrame df = dataSet.loadAsDataFrame();
    }

    @Test
    public void loadBigDecimalLiteral()
    {
        CsvSchema schema = new CsvSchema()
                .addColumn("Name", STRING)
                .addColumn("EmployeeId", LONG)
                .addColumn("Salary", DECIMAL)
                .addColumn("Bonus", DOUBLE)
                ;

        CsvDataSet dataSet = new StringBasedCsvDataSet("Foo", "Employees", schema,
                "Name,EmployeeId,Salary,Bonus\n"
                + "\"Alice\",1234,12000010.1,100.1\n"
                + "\"Bob\",1235,1000005.12,100.2\n"
                + "\"Doris\",1237,100000.701,100.3\n"
        );

        DataFrame df = dataSet.loadAsDataFrame();

        DataFrameUtil.assertEquals(
                new DataFrame("Employees")
                        .addStringColumn("Name").addLongColumn("EmployeeId").addDecimalColumn("Salary").addDoubleColumn("Bonus")
                        .addRow("Alice", 1234, BigDecimal.valueOf(120000101, 1), 100.1)
                        .addRow("Bob",   1235, BigDecimal.valueOf(100000512, 2), 100.2)
                        .addRow("Doris", 1237, BigDecimal.valueOf(100000701, 3), 100.3)
                , df
        );
    }
}
