package io.github.vmzakharov.ecdataframe.dataframe;

import org.eclipse.collections.api.tuple.Twin;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;

import static org.junit.jupiter.api.Assertions.*;

public class DataFrameFilterTest
{
    private DataFrame dataFrameBeep;
    private DataFrame dataFrame;

    @BeforeEach
    public void setUpDataFrame()
    {
        this.dataFrameBeep = new DataFrame("FrameOfData")
            .addStringColumn("Name").addStringColumn("Foo").addLongColumn("Bar").addDoubleColumn("Baz").addDoubleColumn("Qux").addIntColumn("Fred")
            .addRow("Alice",   "Pqr",  11L, 10.0, 20.0, 110)
            .addRow("Albert",  "Abc",  12L, 12.0, 10.0, 120)
            .addRow("Bob",     "Def",  13L, 13.0, 25.0, 130)
            .addRow("Carol",   "Xyz",  14L, 14.0, 40.0, 140)
            .addRow("Abigail", "Def",  15L, 15.0, 11.0, 150)
            ;

        this.dataFrame = this.createBlankDataFrame("FrameOfData")
            .addRow("a",   10L, null, 13.25, 41.0f,  null,  LocalDate.of(2020, 10, 20),                                   null, BigDecimal.valueOf(123, 2))
            .addRow("b",  null,   22, 23.25,  null, false,                        null, LocalDateTime.of(2020, 10, 20, 16, 32), BigDecimal.valueOf(456, 2))
            .addRow(null,  30L,   23,  null, 43.0f, false,  LocalDate.of(2020, 10, 24), LocalDateTime.of(2020, 11, 20, 17, 33),                       null)
            .addRow("d",   40L,   24, 43.25, 44.5f,  true,  LocalDate.of(2020, 10, 26), LocalDateTime.of(2020, 12, 20, 18, 34), BigDecimal.valueOf(901, 2))
            .addRow("e",   50L,   25, 53.25, 48.5f,  true,  LocalDate.of(2020, 11, 26), LocalDateTime.of(2020, 12, 25, 19, 35), BigDecimal.valueOf(101, 2))
            ;
    }

    @Test
    public void simpleSelection()
    {
        DataFrame filtered = this.dataFrame.selectBy("string1 == 'b' or string1 == 'd'");

        DataFrame expected = this.createBlankDataFrame("FrameOfData")
            .addRow("b",  null,   22, 23.25,  null, false,                        null, LocalDateTime.of(2020, 10, 20, 16, 32), BigDecimal.valueOf(456, 2))
            .addRow("d",   40L,   24, 43.25, 44.5f,  true,  LocalDate.of(2020, 10, 26), LocalDateTime.of(2020, 12, 20, 18, 34), BigDecimal.valueOf(901, 2))
            ;

        DataFrameUtil.assertEquals(expected, filtered);

        assertEquals("FrameOfData-selected", filtered.getName());
    }

    @Test
    public void simpleRejection()
    {
        DataFrame filtered = this.dataFrame.rejectBy("string1 == 'b' or string1 == 'd'");

        DataFrame expected = this.createBlankDataFrame("FrameOfData")
            .addRow("a",   10L, null, 13.25, 41.0f,  null,  LocalDate.of(2020, 10, 20),                                   null, BigDecimal.valueOf(123, 2))
            .addRow(null,  30L,   23,  null, 43.0f, false,  LocalDate.of(2020, 10, 24), LocalDateTime.of(2020, 11, 20, 17, 33),                       null)
            .addRow("e",   50L,   25, 53.25, 48.5f,  true,  LocalDate.of(2020, 11, 26), LocalDateTime.of(2020, 12, 25, 19, 35), BigDecimal.valueOf(101, 2))
            ;

        DataFrameUtil.assertEquals(expected, filtered);

        assertEquals("FrameOfData-rejected", filtered.getName());
    }

    @Test
    public void selectedIsOppositeOfRejected()
    {
        String filterExpression = "string1 == 'b' or string1 == 'd'";

        DataFrame selected = this.dataFrame.selectBy(filterExpression);
        DataFrame rejectedOpposite = this.dataFrame.rejectBy("not (" + filterExpression + ")");

        DataFrameUtil.assertEquals(selected, rejectedOpposite);
    }

    @Test
    public void selectionWithComputedFields()
    {
        this.dataFrame.addColumn("Foo", "double1 + float1");
        System.out.println(this.dataFrame.toString());
        DataFrame filtered = this.dataFrame.selectBy("(string1 == 'b' or string1 == 'd' or string1 == 'e') and Foo > 50");
        System.out.println(filtered);

        DataFrame expected = this.createBlankDataFrame("Expected FrameOfData")
            .addRow("d",   40L,   24, 43.25, 44.5f,  true,  LocalDate.of(2020, 10, 26), LocalDateTime.of(2020, 12, 20, 18, 34), BigDecimal.valueOf(901, 2))
            .addRow("e",   50L,   25, 53.25, 48.5f,  true,  LocalDate.of(2020, 11, 26), LocalDateTime.of(2020, 12, 25, 19, 35), BigDecimal.valueOf(101, 2))
            ;

        expected.addColumn("Foo", "double1 + float1");

        DataFrameUtil.assertEquals(expected, filtered);

        assertTrue(filtered.getColumnNamed("Foo").isComputed());
    }

    @Test
    public void partition()
    {
        Twin<DataFrame> selectedAndRejected = this.dataFrame.partition("string1 == 'a' or string1 is null");

        DataFrame expectedSelected = this.createBlankDataFrame("selected")
                .addRow("a",   10L, null, 13.25, 41.0f,  null,  LocalDate.of(2020, 10, 20),                                   null, BigDecimal.valueOf(123, 2))
                .addRow(null,  30L,   23,  null, 43.0f, false,  LocalDate.of(2020, 10, 24), LocalDateTime.of(2020, 11, 20, 17, 33),                       null)
                ;

        DataFrame expectedRejected = this.createBlankDataFrame("rejected")
                .addRow("b",  null,   22, 23.25,  null, false,                        null, LocalDateTime.of(2020, 10, 20, 16, 32), BigDecimal.valueOf(456, 2))
                .addRow("d",   40L,   24, 43.25, 44.5f,  true,  LocalDate.of(2020, 10, 26), LocalDateTime.of(2020, 12, 20, 18, 34), BigDecimal.valueOf(901, 2))
                .addRow("e",   50L,   25, 53.25, 48.5f,  true,  LocalDate.of(2020, 11, 26), LocalDateTime.of(2020, 12, 25, 19, 35), BigDecimal.valueOf(101, 2))
                ;

        DataFrameUtil.assertEquals(expectedSelected, selectedAndRejected.getOne());
        DataFrameUtil.assertEquals(expectedRejected, selectedAndRejected.getTwo());

        assertEquals("FrameOfData-selected", selectedAndRejected.getOne().getName());
        assertEquals("FrameOfData-rejected", selectedAndRejected.getTwo().getName());
    }

    private DataFrame createBlankDataFrame(String name)
    {
        return new DataFrame(name)
            .addStringColumn("string1")
            .addLongColumn("long1").addIntColumn("int1")
            .addDoubleColumn("double1").addFloatColumn("float1")
            .addBooleanColumn("boolean1")
            .addDateColumn("date1").addDateTimeColumn("dateTime1")
            .addDecimalColumn("decimal1");
    }
}
