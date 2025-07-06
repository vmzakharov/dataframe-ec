package io.github.vmzakharov.ecdataframe.dataframe;

import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;

import static org.junit.jupiter.api.Assertions.*;

public class DataFrameToStringTest
{
    @Test
    public void toCsv()
    {
        DataFrame dataFrame = new DataFrame("Employees")
                .addStringColumn("Name")
                .addLongColumn("EmployeeId").addIntColumn("Location").addDateColumn("HireDate").addStringColumn("Dept")
                .addDoubleColumn("Salary").addFloatColumn("Snack Allowance")
                .addDateTimeColumn("Event").addDecimalColumn("Severance")
                .addRow("Alice",  1234, null, LocalDate.of(2020, 1, 1), "Accounting", 110000.0, 10.50f, LocalDateTime.of(2020, 10, 11, 1, 2, 3), BigDecimal.valueOf(1234, 4))
                .addRow("Bob",    null,  102, LocalDate.of(2010, 1, 1), "Bee-bee-boo-boo", 100000.0, null, LocalDateTime.of(2020, 11, 22, 13, 25, 36), null)
                .addRow("Carl",  10000,  103, LocalDate.of(2005, 11, 21), "Controllers", 130001.0, 12.25f, null, BigDecimal.valueOf(1234, 3))
                .addRow("Diane", 10001,  104, LocalDate.of(2012, 9, 20), null, 130000.0, 100.0f, LocalDateTime.of(2022, 8, 21, 4, 5, 6), BigDecimal.valueOf(1234, 2))
                .addRow("Ed",    10002,  105, null, "", 0.0, 10_000.75f, LocalDateTime.of(2022, 10, 11, 21, 32, 53), BigDecimal.valueOf(1234, 1))
                ;

        assertEquals(
                """
                Name,EmployeeId,Location,HireDate,Dept,Salary,Snack Allowance,Event,Severance
                "Alice",1234,,2020-01-01,"Accounting",110000.0,10.5,2020-10-11T01:02:03,0.1234
                "Bob",,102,2010-01-01,"Bee-bee-boo-boo",100000.0,,2020-11-22T13:25:36,
                "Carl",10000,103,2005-11-21,"Controllers",130001.0,12.25,,1.234
                "Diane",10001,104,2012-09-20,,130000.0,100.0,2022-08-21T04:05:06,12.34
                "Ed",10002,105,,"",0.0,10000.75,2022-10-11T21:32:53,123.4
                """,
                dataFrame.asCsvString());
    }

    @Test
    public void toCsvWithLimit()
    {
        DataFrame dataFrame = new DataFrame("Employees")
                .addStringColumn("Name").addLongColumn("EmployeeId").addDateColumn("HireDate").addStringColumn("Dept").addDoubleColumn("Salary")
                .addRow("Alice", 1234, LocalDate.of(2020, 1, 1), "Accounting", 110000.0)
                .addRow("Bob", 1233, LocalDate.of(2010, 1, 1), "Bee-bee-boo-boo", 100000.0)
                .addRow("Carl", 10000, LocalDate.of(2005, 11, 21), "Controllers", 130000.0)
                .addRow("Diane", 10001, LocalDate.of(2012, 9, 20), "", 130000.0)
                .addRow("Ed", 10002, null, "", 0.0)
                ;

        assertEquals(
                """
                Name,EmployeeId,HireDate,Dept,Salary
                "Alice",1234,2020-01-01,"Accounting",110000.0
                "Bob",1233,2010-01-01,"Bee-bee-boo-boo",100000.0
                "Carl",10000,2005-11-21,"Controllers",130000.0
                """
                ,
                dataFrame.asCsvString(3));
    }

    @Test
    public void toCsvWithNulls()
    {
        DataFrame dataFrame = new DataFrame("Employees")
                .addStringColumn("Name").addLongColumn("EmployeeId").addDateColumn("HireDate").addStringColumn("Dept").addDoubleColumn("Salary");

        dataFrame
                .addRow("Alice", 1234, LocalDate.of(2020, 1, 1), "Accounting", 110000.0)
                .addRow("Bob", null, LocalDate.of(2010, 1, 1), "Bee-bee-boo-boo", 100000.0)
                .addRow("Carl", 10000, null, "Controllers", 130000.0)
                .addRow("Diane", 10001, LocalDate.of(2012, 9, 20), "", null)
                .addRow("Ed", 10002, null, "", 0.0)
                ;

        assertEquals(
                """
                Name,EmployeeId,HireDate,Dept,Salary
                "Alice",1234,2020-01-01,"Accounting",110000.0
                "Bob",,2010-01-01,"Bee-bee-boo-boo",100000.0
                "Carl",10000,,"Controllers",130000.0
                "Diane",10001,2012-09-20,"",
                "Ed",10002,,"",0.0
                """
                ,
                dataFrame.asCsvString());
    }

    @Test
    public void toStringMethod()
    {
        DataFrame dataFrame = new DataFrame("Employees")
                .addStringColumn("Name").addLongColumn("EmployeeId").addDateColumn("HireDate").addStringColumn("Dept")
                .addRow("Alice", 1234, LocalDate.of(2020, 1, 1), "Accounting")
                .addRow("Bob", 1233, LocalDate.of(2010, 1, 1), "Bee-bee-boo-boo")
                .addRow("Carl", 10000, LocalDate.of(2005, 11, 21), "Controllers")
                .addRow("Diane", 10001, LocalDate.of(2012, 9, 20), "IT")
                .addRow("Ed", 10002, LocalDate.of(2013, 9, 20), "Accounting")
                .addRow("Frank", 10003, null, "Event Planning")
                .addRow("Grace", 10004, LocalDate.of(2012, 10, 20), "")
                .addRow("Heidi", 10005, LocalDate.of(2023, 6, 20), null)
                .addRow("Ivan", 10006, LocalDate.of(2012, 12, 22), "")
                .addRow("Judy", 10007, LocalDate.of(2017, 1, 2), "")
                .addRow("Mike", 10008, LocalDate.of(2011, 11, 12), "")
                ;

        String expected = "Employees [11 rows]\n"
                + dataFrame.asCsvString(10)
                + "...\n";

        assertEquals(expected, dataFrame.toString());
    }
}
