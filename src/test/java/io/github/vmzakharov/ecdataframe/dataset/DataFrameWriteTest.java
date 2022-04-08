package io.github.vmzakharov.ecdataframe.dataset;

import io.github.vmzakharov.ecdataframe.dataframe.DataFrame;
import io.github.vmzakharov.ecdataframe.dsl.value.ValueType;
import org.junit.Assert;
import org.junit.Test;

import java.time.LocalDate;
import java.time.LocalDateTime;

public class DataFrameWriteTest
{
    @Test
    public void writeNoSchema()
    {
        DataFrame dataFrame = new DataFrame("source")
                .addStringColumn("Name").addLongColumn("EmployeeId").addDateColumn("HireDate").addStringColumn("Dept").addDoubleColumn("Salary")
                .addRow("Alice", 1234, LocalDate.of(2020, 1, 1), "Accounting", 110000.0)
                .addRow("Bob", 1233, LocalDate.of(2010, 1, 1), "Bee-bee-boo-boo", 100000.0)
                .addRow("Carl", 10000, LocalDate.of(2005, 11, 21), "Controllers", 130000.0)
                .addRow("Diane", 10001, LocalDate.of(2012, 9, 20), "", null)
                .addRow("Ed", 10002, null, null, 0.0)
                ;

        StringBasedCsvDataSet dataSet = new StringBasedCsvDataSet("Foo", "Employees", "");
        dataSet.write(dataFrame);

        String expected = "Name,EmployeeId,HireDate,Dept,Salary\n"
                        + "\"Alice\",1234,2020-1-1,\"Accounting\",110000.0\n"
                        + "\"Bob\",1233,2010-1-1,\"Bee-bee-boo-boo\",100000.0\n"
                        + "\"Carl\",10000,2005-11-21,\"Controllers\",130000.0\n"
                        + "\"Diane\",10001,2012-9-20,\"\",\n"
                        + "\"Ed\",10002,,,0.0\n";

        Assert.assertEquals(expected, dataSet.getWrittenData());
    }

    @Test
    public void writeWithSchema()
    {
        CsvSchema schema = new CsvSchema();
        schema.addColumn("Name",       ValueType.STRING);
        schema.addColumn("EmployeeId", ValueType.LONG);
        schema.addColumn("HireDate",   ValueType.DATE, "uuuu-MM-dd");
        schema.addColumn("OtherDate",  ValueType.DATE, "M/d/uuuu");
        schema.addColumn("Dept",       ValueType.STRING);
        schema.addColumn("Salary",     ValueType.DOUBLE);
        schema.quoteCharacter('\'');

        DataFrame dataFrame = new DataFrame("Employees")
                .addStringColumn("Name").addLongColumn("EmployeeId").addDateColumn("HireDate").addDateColumn("OtherDate").addStringColumn("Dept").addDoubleColumn("Salary")
                .addRow("Alice", 1234, LocalDate.of(2020, 1, 1), LocalDate.of(2021, 1, 1), "Accounting", 110000.0)
                .addRow("Bob", 1233, LocalDate.of(2010, 1, 1), LocalDate.of(2021, 1, 1), "Bee-bee-boo-boo", 100000.0)
                .addRow("Carl", null, LocalDate.of(2005, 11, 21), LocalDate.of(2021, 11, 21), "Controllers", null)
                .addRow("Diane", 10001, LocalDate.of(2012, 9, 20), LocalDate.of(2022, 9, 20), "", 130000.0)
                .addRow("Ed", 10002, null, null, null, 0.0)
                ;

        StringBasedCsvDataSet dataSet = new StringBasedCsvDataSet("Foo", "Employees", schema, "");
        dataSet.write(dataFrame);

        String expected = "Name,EmployeeId,HireDate,OtherDate,Dept,Salary\n"
                + "'Alice',1234,2020-01-01,1/1/2021,'Accounting',110000.0\n"
                + "'Bob',1233,2010-01-01,1/1/2021,'Bee-bee-boo-boo',100000.0\n"
                + "'Carl',,2005-11-21,11/21/2021,'Controllers',\n"
                + "'Diane',10001,2012-09-20,9/20/2022,'',130000.0\n"
                + "'Ed',10002,,,,0.0\n";

        Assert.assertEquals(expected, dataSet.getWrittenData());
    }

    @Test
    public void writeWithSchemaPipeNullMarkers()
    {
        CsvSchema schema = new CsvSchema();
        schema.addColumn("Name",       ValueType.STRING);
        schema.addColumn("EmployeeId", ValueType.LONG);
        schema.addColumn("HireDate",   ValueType.DATE, "uuuu-MM-dd");
        schema.addColumn("Dept",       ValueType.STRING);
        schema.addColumn("Salary",     ValueType.DOUBLE);
        schema.quoteCharacter('\'');
        schema.separator('|');
        schema.nullMarker("-null-");

        DataFrame dataFrame = new DataFrame("Employees")
                .addStringColumn("Name").addLongColumn("EmployeeId").addDateColumn("HireDate").addStringColumn("Dept").addDoubleColumn("Salary")
                .addRow("Alice", 1234, LocalDate.of(2020, 1, 1), "Accounting", 110000.0)
                .addRow("Bob", 1233, null, "Bee-bee-boo-boo", 100000.0)
                .addRow("Carl", 10000, LocalDate.of(2005, 11, 21), "Controllers", null)
                .addRow("Diane", 10001, LocalDate.of(2012, 9, 20), "", 130000.0)
                .addRow("Ed", 10002, null, null, 0.0)
                ;

        StringBasedCsvDataSet dataSet = new StringBasedCsvDataSet("Foo", "Employees", schema, "");
        dataSet.write(dataFrame);

        String expected = "Name|EmployeeId|HireDate|Dept|Salary\n"
                + "'Alice'|1234|2020-01-01|'Accounting'|110000.0\n"
                + "'Bob'|1233|-null-|'Bee-bee-boo-boo'|100000.0\n"
                + "'Carl'|10000|2005-11-21|'Controllers'|-null-\n"
                + "'Diane'|10001|2012-09-20|''|130000.0\n"
                + "'Ed'|10002|-null-|-null-|0.0\n";

        Assert.assertEquals(expected, dataSet.getWrittenData());
    }

    @Test
    public void writeDateTime()
    {
        DataFrame dataFrame = new DataFrame("PeopleAndDateTimes")
                .addStringColumn("Name").addDateTimeColumn("DateTime")
                .addRow("Alice", LocalDateTime.of(2020,  9, 22, 13, 14, 15))
                .addRow("Bob",   LocalDateTime.of(2021, 10, 23, 13, 14, 16))
                .addRow("Carl", null)
                .addRow("Diane", LocalDateTime.of(2023, 12, 24, 13, 14, 15)
                );

        StringBasedCsvDataSet dataSet = new StringBasedCsvDataSet("Foo", "People",  "");
        dataSet.write(dataFrame);

        String expected = "Name,DateTime\n"
                        + "\"Alice\",2020-9-22T13:14:15\n"
                        + "\"Bob\",2021-10-23T13:14:16\n"
                        + "\"Carl\",\n"
                        + "\"Diane\",2023-12-24T13:14:15\n";

        Assert.assertEquals(expected, dataSet.getWrittenData());
    }

    @Test
    public void writeDateTimeWithSchema()
    {
        DataFrame dataFrame = new DataFrame("PeopleAndDateTimes")
                .addStringColumn("Name").addDateTimeColumn("DateTime")
                .addRow("Alice", LocalDateTime.of(2020,  9, 22, 13, 14, 15))
                .addRow("Bob",   LocalDateTime.of(2021, 10, 23, 13, 14, 16))
                .addRow("Carl", null)
                .addRow("Diane", LocalDateTime.of(2023, 12, 24, 13, 14, 15)
                );

        dataFrame.addDateTimeColumn("LooksDifferent", "DateTime");

        CsvSchema schema = new CsvSchema();
        schema.addColumn("Name",           ValueType.STRING);
        schema.addColumn("DateTime",       ValueType.DATE_TIME, "uuuu-MM-dd'/'HH:mm:ss");
        schema.addColumn("LooksDifferent", ValueType.DATE_TIME, "uuuu*M*d-HH*mm*ss");

        StringBasedCsvDataSet dataSet = new StringBasedCsvDataSet("Foo", "People", schema, "");
        dataSet.write(dataFrame);

        String expected = "Name,DateTime,LooksDifferent\n"
                        + "\"Alice\",2020-09-22/13:14:15,2020*9*22-13*14*15\n"
                        + "\"Bob\",2021-10-23/13:14:16,2021*10*23-13*14*16\n"
                        + "\"Carl\",,\n"
                        + "\"Diane\",2023-12-24/13:14:15,2023*12*24-13*14*15\n";

        Assert.assertEquals(expected, dataSet.getWrittenData());
    }
}
