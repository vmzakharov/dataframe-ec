package io.github.vmzakharov.ecdataframe.dataframe;

import org.junit.Test;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;

public class DataFrameComputedWithInferredTypeTest
{
    @Test
    public void computedString()
    {
        DataFrame df = new DataFrame("Frame")
                .addStringColumn("Foo").addStringColumn("Bar").addLongColumn("Baz")
                .addRow("Alice", "A",   12)
                .addRow("Bob",   "B",   14)
                .addRow("Carl",   null, 16)
                .addRow("Doris", "D",   18)
                ;

        df.addColumn("Waldo", "Foo + Bar");

        DataFrameUtil.assertEquals(new DataFrame("Frame")
                .addStringColumn("Foo").addStringColumn("Bar").addLongColumn("Baz").addStringColumn("Waldo")
                .addRow("Alice", "A", 12, "AliceA")
                .addRow("Bob",   "B", 14, "BobB")
                .addRow("Carl", null, 16, null)
                .addRow("Doris", "D", 18, "DorisD")
                ,
                df);
    }

    @Test
    public void computedLong()
    {
        DataFrame df = new DataFrame("Frame")
                .addStringColumn("Foo").addStringColumn("Bar").addLongColumn("Baz")
                .addRow("Alice", "10",  12)
                .addRow("Bob",   "10",  14)
                .addRow("Carl",   null, 16)
                .addRow("Doris", "10",  18)
                ;

        df.addColumn("Waldo", "toLong(Bar) + Baz");

        DataFrameUtil.assertEquals(new DataFrame("Frame")
                .addStringColumn("Foo").addStringColumn("Bar").addLongColumn("Baz").addLongColumn("Waldo")
                .addRow("Alice", "10",  12, 22)
                .addRow("Bob",   "10",  14, 24)
                .addRow("Carl",   null, 16, null)
                .addRow("Doris", "10",  18, 28)
                ,
                df);
    }

    @Test
    public void computedInt()
    {
        DataFrame df = new DataFrame("Frame")
                .addStringColumn("Foo").addStringColumn("Bar").addIntColumn("Baz")
                .addRow("Alice", "10",  12)
                .addRow("Bob",   "10",  14)
                .addRow("Carl",   null, 16)
                .addRow("Doris", "10",  18)
                ;

        df.addColumn("Waldo", "toLong(Bar) + Baz");

        DataFrameUtil.assertEquals(new DataFrame("Frame")
                        .addStringColumn("Foo").addStringColumn("Bar").addIntColumn("Baz").addLongColumn("Waldo")
                        .addRow("Alice", "10",  12, 22)
                        .addRow("Bob",   "10",  14, 24)
                        .addRow("Carl",   null, 16, null)
                        .addRow("Doris", "10",  18, 28)
                ,
                df);
    }

    @Test
    public void computedDouble()
    {
        DataFrame df = new DataFrame("Frame")
                .addStringColumn("Foo").addStringColumn("Bar").addLongColumn("Baz")
                .addRow("Alice", "12.0",  12)
                .addRow("Bob",   "11.0",  14)
                .addRow("Carl",   null,   16)
                .addRow("Doris", "10.0",  18)
                ;

        df.addColumn("Waldo", "toDouble(Bar) + Baz");

        DataFrameUtil.assertEquals(new DataFrame("Frame")
                .addStringColumn("Foo").addStringColumn("Bar").addLongColumn("Baz").addDoubleColumn("Waldo")
                .addRow("Alice", "12.0", 12, 24.0)
                .addRow("Bob",   "11.0", 14, 25.0)
                .addRow("Carl",   null,  16, null)
                .addRow("Doris", "10.0", 18, 28.0)
                ,
                df);
    }

    @Test
    public void computedDecimal()
    {
        DataFrame df = new DataFrame("Frame")
                .addStringColumn("Foo").addLongColumn("Bar").addLongColumn("Baz").addDecimalColumn("Qux")
                .addRow("Alice", 100,  2, BigDecimal.valueOf(12, 1))
                .addRow("Bob",   101,  2, BigDecimal.valueOf(14, 1))
                .addRow("Carl",  null, 2, BigDecimal.valueOf(16, 1))
                .addRow("Doris", 102,  2, BigDecimal.valueOf(18, 1))
                ;

        df.addColumn("Waldo", "toDecimal(Bar, Baz) + Qux");

        DataFrameUtil.assertEquals(new DataFrame("Frame")
                .addStringColumn("Foo").addLongColumn("Bar").addLongColumn("Baz").addDecimalColumn("Qux").addDecimalColumn("Waldo")
                .addRow("Alice", 100,  2, BigDecimal.valueOf(12, 1), BigDecimal.valueOf(22, 1))
                .addRow("Bob",   101,  2, BigDecimal.valueOf(14, 1), BigDecimal.valueOf(241, 2))
                .addRow("Carl",  null, 2, BigDecimal.valueOf(16, 1), null)
                .addRow("Doris", 102,  2, BigDecimal.valueOf(18, 1), BigDecimal.valueOf(282, 2))
                ,
                df);
    }

    @Test
    public void computedDate()
    {
        DataFrame df = new DataFrame("Frame")
                .addStringColumn("Foo").addLongColumn("Year").addLongColumn("Month").addLongColumn("Day").addStringColumn("DateAsString")
                .addRow("Alice", 2020,   10, 17, "2020-11-12")
                .addRow("Bob",   2021,   11, 27, null)
                .addRow("Carl",  2022, null,  7, "2020-11-12")
                .addRow("Doris", 2023,    9, 22, "2020-11-12")
                ;

        df.addColumn("Waldo", "toDate(Year, Month, Day)");
        df.addColumn("Qux", "toDate(DateAsString)");

        DataFrameUtil.assertEquals(new DataFrame("Frame")
                .addStringColumn("Foo").addLongColumn("Year").addLongColumn("Month").addLongColumn("Day").addStringColumn("DateAsString")
                .addDateColumn("Waldo").addDateColumn("Qux")
                .addRow("Alice", 2020,   10, 17,  "2020-11-12", LocalDate.of(2020, 10, 17), LocalDate.of(2020, 11, 12))
                .addRow("Bob",   2021,   11, 27,  null,         LocalDate.of(2021, 11, 27), null)
                .addRow("Carl",  2022, null,  7,  "2020-11-12", null,                       LocalDate.of(2020, 11, 12))
                .addRow("Doris", 2023,    9, 22,  "2020-11-12", LocalDate.of(2023,  9, 22), LocalDate.of(2020, 11, 12))
                ,
                df);
    }

    @Test
    public void computedDateTime()
    {
        DataFrame df = new DataFrame("Frame")
                .addStringColumn("Foo")
                .addLongColumn("Year").addLongColumn("Month").addLongColumn("Day")
                .addLongColumn("Hour").addLongColumn("Minute").addLongColumn("Second").addLongColumn("NanoSecond")
                .addStringColumn("DateTimeAsString")
                .addRow("Alice", 2020,   10, 17, 10, 10, 21, 123, "2020-11-12T10:10:21")
                .addRow("Bob",   2021,   11, 27, 11, 20, 22, 456, null)
                .addRow("Carl",  2022, null,  7, 12, 30, 23, 789, "2020-11-12T12:30:23")
                .addRow("Doris", 2023,    9, 22, 13, 40, 24, 231, "2020-11-12T13:40:24")
                ;

        df.addColumn("Waldo", "toDateTime(Year, Month, Day, Hour, Minute, Second, NanoSecond)");
        df.addColumn("Qux", "toDateTime(DateTimeAsString)");

        DataFrame expected = new DataFrame("Expected Frame")
                .addStringColumn("Foo")
                .addLongColumn("Year").addLongColumn("Month").addLongColumn("Day")
                .addLongColumn("Hour").addLongColumn("Minute").addLongColumn("Second").addLongColumn("NanoSecond")
                .addStringColumn("DateTimeAsString")
                .addDateTimeColumn("Waldo").addDateTimeColumn("Qux")
                .addRow("Alice", 2020, 10,   17, 10, 10, 21, 123, "2020-11-12T10:10:21",  LocalDateTime.of(2020, 10, 17, 10, 10, 21, 123), LocalDateTime.of(2020, 11, 12, 10, 10, 21))
                .addRow("Bob",   2021, 11,   27, 11, 20, 22, 456,   null,                 LocalDateTime.of(2021, 11, 27, 11, 20, 22, 456), null)
                .addRow("Carl",  2022, null,  7, 12, 30, 23, 789, "2020-11-12T12:30:23",  null,                                            LocalDateTime.of(2020, 11, 12, 12, 30, 23))
                .addRow("Doris", 2023, 9,    22, 13, 40, 24, 231,  "2020-11-12T13:40:24", LocalDateTime.of(2023, 9, 22, 13, 40, 24, 231),  LocalDateTime.of(2020, 11, 12, 13, 40, 24));

        DataFrameUtil.assertEquals(expected, df);
    }

    @Test(expected = RuntimeException.class)
    public void typeMismatch()
    {
        DataFrame df = new DataFrame("Frame")
                .addStringColumn("Foo").addStringColumn("Bar").addLongColumn("Baz")
                .addRow("Alice", "A",   12)
                .addRow("Bob",   "B",   14)
                .addRow("Carl",   null, 16)
                .addRow("Doris", "D",   18)
                ;

        df.addColumn("Waldo", "Bar + Baz");
    }
}
