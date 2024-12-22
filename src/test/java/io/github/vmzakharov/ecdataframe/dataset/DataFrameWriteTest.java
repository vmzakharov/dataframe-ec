package io.github.vmzakharov.ecdataframe.dataset;

import io.github.vmzakharov.ecdataframe.dataframe.DataFrame;
import io.github.vmzakharov.ecdataframe.dsl.value.ValueType;

import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;

import static io.github.vmzakharov.ecdataframe.dsl.value.ValueType.*;
import static org.junit.jupiter.api.Assertions.*;

public class DataFrameWriteTest
{
    @Test
    public void writeNoSchema()
    {
        DataFrame dataFrame = new DataFrame("source")
                .addStringColumn("Name").addLongColumn("EmployeeId").addDateColumn("HireDate").addStringColumn("Dept").addDoubleColumn("Salary")
                .addIntColumn("PetCount").addFloatColumn("LunchAllowance")
                .addRow("Alice", 1234, LocalDate.of(2020, 1, 1), "Accounting", 110000.0, 12, 20.25f)
                .addRow("Bob", 1233, LocalDate.of(2010, 1, 1), "Bee-bee-boo-boo", 100000.0, -10, -10.75f)
                .addRow("Carl", 10000, LocalDate.of(2005, 11, 21), "Controllers", 130000.0, null, 15.25f)
                .addRow("Diane", 10001, LocalDate.of(2012, 9, 20), "", null, 10, 4.5f)
                .addRow("Ed", 10002, null, null, 0.0, null, null)
                ;

        StringBasedCsvDataSet dataSet = new StringBasedCsvDataSet("Foo", "Employees", "");
        dataSet.write(dataFrame);

        String expected = """
                Name,EmployeeId,HireDate,Dept,Salary,PetCount,LunchAllowance
                "Alice",1234,2020-1-1,"Accounting",110000.0,12,20.25
                "Bob",1233,2010-1-1,"Bee-bee-boo-boo",100000.0,-10,-10.75
                "Carl",10000,2005-11-21,"Controllers",130000.0,,15.25
                "Diane",10001,2012-9-20,"",,10,4.5
                "Ed",10002,,,0.0,,
                """;

        assertEquals(expected, dataSet.getWrittenData());
    }

    @Test
    public void writeNoSchemaIntEmployeeId()
    {
        DataFrame dataFrame = new DataFrame("source")
                .addStringColumn("Name").addIntColumn("EmployeeId").addDateColumn("HireDate").addStringColumn("Dept").addDoubleColumn("Salary")
                .addRow("Alice", 1234, LocalDate.of(2020, 1, 1), "Accounting", 110000.0)
                .addRow("Bob", 1233, LocalDate.of(2010, 1, 1), "Bee-bee-boo-boo", 100000.0)
                .addRow("Carl", 10000, LocalDate.of(2005, 11, 21), "Controllers", 130000.0)
                .addRow("Diane", 10001, LocalDate.of(2012, 9, 20), "", null)
                .addRow("Ed", 10002, null, null, 0.0)
                ;

        StringBasedCsvDataSet dataSet = new StringBasedCsvDataSet("Foo", "Employees", "");
        dataSet.write(dataFrame);

        String expected = """
                Name,EmployeeId,HireDate,Dept,Salary
                "Alice",1234,2020-1-1,"Accounting",110000.0
                "Bob",1233,2010-1-1,"Bee-bee-boo-boo",100000.0
                "Carl",10000,2005-11-21,"Controllers",130000.0
                "Diane",10001,2012-9-20,"",
                "Ed",10002,,,0.0
                """;

        assertEquals(expected, dataSet.getWrittenData());
    }

    @Test
    public void writeWithSchema()
    {
        CsvSchema schema = new CsvSchema();
        schema.addColumn("Name",           ValueType.STRING);
        schema.addColumn("EmployeeId",     ValueType.LONG);
        schema.addColumn("HireDate",       ValueType.DATE, "uuuu-MM-dd");
        schema.addColumn("OtherDate",      ValueType.DATE, "M/d/uuuu");
        schema.addColumn("Dept",           ValueType.STRING);
        schema.addColumn("Salary",         ValueType.DOUBLE);
        schema.addColumn("PetCount",       ValueType.INT);
        schema.addColumn("LunchAllowance", ValueType.FLOAT);
        schema.quoteCharacter('\'');

        DataFrame dataFrame = new DataFrame("Employees")
                .addStringColumn("Name").addLongColumn("EmployeeId").addDateColumn("HireDate").addDateColumn("OtherDate")
                .addStringColumn("Dept").addDoubleColumn("Salary")
                .addIntColumn("PetCount").addFloatColumn("LunchAllowance")
                .addRow("Alice", 1234, LocalDate.of(2020, 1, 1), LocalDate.of(2021, 1, 1), "Accounting", 110000.0, 12, 20.25f)
                .addRow("Bob", 1233, LocalDate.of(2010, 1, 1), LocalDate.of(2021, 1, 1), "Bee-bee-boo-boo", 100000.0, -10, -10.75f)
                .addRow("Carl", null, LocalDate.of(2005, 11, 21), LocalDate.of(2021, 11, 21), "Controllers", null, null, 15.25f)
                .addRow("Diane", 10001, LocalDate.of(2012, 9, 20), LocalDate.of(2022, 9, 20), "", 130000.0, 10, 4.5f)
                .addRow("Ed", 10002, null, null, null, 0.0, null, null)
                ;

        StringBasedCsvDataSet dataSet = new StringBasedCsvDataSet("Foo", "Employees", schema, "");
        dataSet.write(dataFrame);

        String expected = """
                Name,EmployeeId,HireDate,OtherDate,Dept,Salary,PetCount,LunchAllowance
                'Alice',1234,2020-01-01,1/1/2021,'Accounting',110000.0,12,20.25
                'Bob',1233,2010-01-01,1/1/2021,'Bee-bee-boo-boo',100000.0,-10,-10.75
                'Carl',,2005-11-21,11/21/2021,'Controllers',,,15.25
                'Diane',10001,2012-09-20,9/20/2022,'',130000.0,10,4.5
                'Ed',10002,,,,0.0,,
                """
                ;

        assertEquals(expected, dataSet.getWrittenData());
    }

    @Test
    public void writeWithSchemaWithPipeAndNullMarkers()
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

        String expected = """
                Name|EmployeeId|HireDate|Dept|Salary
                'Alice'|1234|2020-01-01|'Accounting'|110000.0
                'Bob'|1233|-null-|'Bee-bee-boo-boo'|100000.0
                'Carl'|10000|2005-11-21|'Controllers'|-null-
                'Diane'|10001|2012-09-20|''|130000.0
                'Ed'|10002|-null-|-null-|0.0
                """;

        assertEquals(expected, dataSet.getWrittenData());
    }

    @Test
    public void writeBigDecimal()
    {
        DataFrame dataFrame = new DataFrame("Employees")
            .addStringColumn("Name").addLongColumn("EmployeeId").addDecimalColumn("Salary").addDoubleColumn("Bonus")
            .addRow("Alice", 1234, BigDecimal.valueOf(120000101, 1), 100.1)
            .addRow("Bob",   1235,                             null, 100.2)
            .addRow("Doris", 1237, BigDecimal.valueOf(100000701, 3), 100.3)
            ;

        dataFrame.addColumn("TwoSalaries", "Salary * 2");

        CsvSchema schema = new CsvSchema()
            .addColumn("Name", STRING)
            .addColumn("EmployeeId", LONG)
            .addColumn("Salary", DECIMAL)
            .addColumn("Bonus", DOUBLE)
            .addColumn("TwoSalaries", DECIMAL, "\"$#,##0.00\"")
            ;

        StringBasedCsvDataSet dataSet = new StringBasedCsvDataSet("Foo", "Employees", schema, "");

        dataSet.write(dataFrame);

        String expected = """
            Name,EmployeeId,Salary,Bonus,TwoSalaries
            "Alice",1234,12000010.1,100.1,"$24,000,020.20"
            "Bob",1235,,100.2,
            "Doris",1237,100000.701,100.3,"$200,001.40"
            """;

        assertEquals(expected, dataSet.getWrittenData());
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

        String expected = """
                Name,DateTime
                "Alice",2020-9-22T13:14:15
                "Bob",2021-10-23T13:14:16
                "Carl",
                "Diane",2023-12-24T13:14:15
                """;

        assertEquals(expected, dataSet.getWrittenData());
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

        String expected = """
                Name,DateTime,LooksDifferent
                "Alice",2020-09-22/13:14:15,2020*9*22-13*14*15
                "Bob",2021-10-23/13:14:16,2021*10*23-13*14*16
                "Carl",,
                "Diane",2023-12-24/13:14:15,2023*12*24-13*14*15
                """;

        assertEquals(expected, dataSet.getWrittenData());
    }

    @Test
    public void writeWithoutHeaderLine()
    {
        DataFrame dataFrame = new DataFrame("Employees")
                .addStringColumn("Name").addLongColumn("EmployeeId").addDateColumn("HireDate").addDoubleColumn("Salary").addLongColumn("DeptCode")
                .addRow("Bob", 1235, LocalDate.of(2010, 1, 1), 100_000.50, 12)
                .addRow("Doris", 1237, LocalDate.of(2010, 1, 1), 100_000.70, 15)
                ;

        CsvSchema schema = new CsvSchema()
                .addColumn("Name", STRING)
                .addColumn("EmployeeId", LONG)
                .addColumn("HireDate", DATE, "uuuu-MM-dd")
                .addColumn("Salary", DOUBLE)
                .addColumn("DeptCode", LONG)
                .hasHeaderLine(false);

        StringBasedCsvDataSet dataSet = new StringBasedCsvDataSet("Foo", "Employees", schema, "");
        dataSet.write(dataFrame);

        String expected =
                """
                "Bob",1235,2010-01-01,100000.5,12
                "Doris",1237,2010-01-01,100000.7,15
                """;

        assertEquals(expected, dataSet.getWrittenData());
    }
}
