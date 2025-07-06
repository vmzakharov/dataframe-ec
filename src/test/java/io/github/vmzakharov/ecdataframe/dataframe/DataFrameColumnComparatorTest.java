package io.github.vmzakharov.ecdataframe.dataframe;

import io.github.vmzakharov.ecdataframe.dataframe.compare.ComparisonResult;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;

import static org.junit.jupiter.api.Assertions.*;

public class DataFrameColumnComparatorTest
{
    public static final double TOLERANCE = 0.00000001;

    public static DataFrame df1;
    public static DataFrame df2;

    @BeforeAll
    public static void initializeDataFrames()
    {
        df1 = new DataFrame("df1")
            .addStringColumn("string1")
            .addLongColumn("long1").addIntColumn("int1")
            .addDoubleColumn("double1").addFloatColumn("float1")
            .addBooleanColumn("boolean1")
            .addDateColumn("date1").addDateTimeColumn("dateTime1")
            .addDecimalColumn("decimal1")
            .addRow("a",   10L, null, 13.25, 41.0f,  null,  LocalDate.of(2020, 10, 20),                                   null, BigDecimal.valueOf(123, 2))
            .addRow("b",  null,   22, 23.25,  null, false,                        null, LocalDateTime.of(2020, 10, 20, 16, 32), BigDecimal.valueOf(456, 2))
            .addRow(null,  30L,   23,  null, 43.0f, false,  LocalDate.of(2020, 10, 24), LocalDateTime.of(2020, 11, 20, 17, 33),                       null)
            .addRow("d",   40L,   24, 43.25, 44.5f,  true,  LocalDate.of(2020, 10, 26), LocalDateTime.of(2020, 12, 20, 18, 34), BigDecimal.valueOf(901, 2))
            ;

        df2 = new DataFrame("df2")
            .addStringColumn("string2")
            .addLongColumn("long2").addIntColumn("int2")
            .addDoubleColumn("double2").addFloatColumn("float2")
            .addBooleanColumn("boolean2")
            .addDateColumn("date2").addDateTimeColumn("dateTime2")
            .addDecimalColumn("decimal2")
            .addRow("a",  10L,   21, 13.25, 41.0f,  true,  LocalDate.of(2020, 10, 20), LocalDateTime.of(2020,  9, 20, 15, 31), BigDecimal.valueOf(123, 2))
            .addRow(null, 20L,   22,  null, 42.0f, false,                        null, LocalDateTime.of(2020, 10, 20, 16, 32), BigDecimal.valueOf(456, 2))
            .addRow("c", null,   23, 33.50,  null, false,  LocalDate.of(2020, 10, 24),                                   null, BigDecimal.valueOf(789, 2))
            .addRow("d",  40L, null, 43.25, 44.5f,  null,  LocalDate.of(2020, 10, 26), LocalDateTime.of(2020, 12, 20, 18, 34),                       null)
            ;
    }

    @Test
    public void stringComparator()
    {
        DfCellComparator cellComparator = df1.columnComparator(df2, "string1", "string2");
        this.valueEqualsValue(cellComparator.compare(0, 0));
        this.valueAndNull(cellComparator.compare(0, 1));
        this.nullAndValue(cellComparator.compare(2, 0));
        this.bothAreNulls(cellComparator.compare(2, 1));
        this.leftGreaterThanRight(cellComparator.compare(3, 0), "d".compareTo("a"));
        this.leftLessThanRight(cellComparator.compare(0, 3), "a".compareTo("d"));
    }

    @Test
    public void longComparator()
    {
        DfCellComparator cellComparator = df1.columnComparator(df2, "long1", "long2");
        this.valueEqualsValue(cellComparator.compare(0, 0));
        this.valueAndNull(cellComparator.compare(0, 2));
        this.nullAndValue(cellComparator.compare(1, 0));
        this.bothAreNulls(cellComparator.compare(1, 2));
        this.leftGreaterThanRight(cellComparator.compare(3, 0), 40L - 10L);
        this.leftLessThanRight(cellComparator.compare(0, 3), 10L - 40L);
    }

    @Test
    public void intComparator()
    {
        DfCellComparator cellComparator = df1.columnComparator(df2, "int1", "int2");
        this.valueEqualsValue(cellComparator.compare(1, 1));
        this.valueAndNull(cellComparator.compare(1, 3));
        this.nullAndValue(cellComparator.compare(0, 1));
        this.bothAreNulls(cellComparator.compare(0, 3));
        this.leftGreaterThanRight(cellComparator.compare(2, 1), 23 - 22);
        this.leftLessThanRight(cellComparator.compare(1, 2), 22 - 23);
    }

    @Test
    public void doubleComparator()
    {
        DfCellComparator cellComparator = df1.columnComparator(df2, "double1", "double2");
        this.valueEqualsValue(cellComparator.compare(0, 0));
        this.valueAndNull(cellComparator.compare(0, 1));
        this.nullAndValue(cellComparator.compare(2, 0));
        this.bothAreNulls(cellComparator.compare(2, 1));
        this.leftGreaterThanRight(cellComparator.compare(3, 0), 43.25 - 13.25);
        this.leftLessThanRight(cellComparator.compare(0, 3), 13.25 - 43.25);
    }

    @Test
    public void floatComparator()
    {
        DfCellComparator cellComparator = df1.columnComparator(df2, "float1", "float2");
        this.valueEqualsValue(cellComparator.compare(0, 0));
        this.valueAndNull(cellComparator.compare(0, 2));
        this.nullAndValue(cellComparator.compare(1, 0));
        this.bothAreNulls(cellComparator.compare(1, 2));
        this.leftGreaterThanRight(cellComparator.compare(3, 0), 44.5 - 41.0);
        this.leftLessThanRight(cellComparator.compare(0, 3), 41.0 - 44.5);
    }

    @Test
    public void booleanComparator()
    {
        DfCellComparator cellComparator = df1.columnComparator(df2, "boolean1", "boolean2");
        this.valueEqualsValue(cellComparator.compare(2, 2));
        this.valueAndNull(cellComparator.compare(1, 3));
        this.nullAndValue(cellComparator.compare(0, 1));
        this.bothAreNulls(cellComparator.compare(0, 3));
        this.leftGreaterThanRight(cellComparator.compare(3, 1), Boolean.compare(true, false));
        this.leftLessThanRight(cellComparator.compare(1, 0), Boolean.compare(false, true));
    }

    @Test
    public void dateComparator()
    {
        DfCellComparator cellComparator = df1.columnComparator(df2, "date1", "date2");
        this.valueEqualsValue(cellComparator.compare(0, 0));
        this.valueAndNull(cellComparator.compare(0, 1));
        this.nullAndValue(cellComparator.compare(1, 0));
        this.bothAreNulls(cellComparator.compare(1, 1));
        this.leftGreaterThanRight(cellComparator.compare(2, 0), LocalDate.of(2020, 10, 24).compareTo(LocalDate.of(2020, 10, 20)));
        this.leftLessThanRight(cellComparator.compare(0, 2), LocalDate.of(2020, 10, 20).compareTo(LocalDate.of(2020, 10, 24)));
    }

    @Test
    public void dateTimeComparator()
    {
        DfCellComparator cellComparator = df1.columnComparator(df2, "dateTime1", "dateTime2");
        this.valueEqualsValue(cellComparator.compare(1, 1));
        this.valueAndNull(cellComparator.compare(1, 2));
        this.nullAndValue(cellComparator.compare(0, 1));
        this.bothAreNulls(cellComparator.compare(0, 2));
        this.leftGreaterThanRight(cellComparator.compare(3, 1), LocalDateTime.of(2020, 12, 20, 18, 34).compareTo(LocalDateTime.of(2020, 10, 20, 16, 32)));
        this.leftLessThanRight(cellComparator.compare(1, 3), LocalDateTime.of(2020, 10, 20, 16, 32).compareTo(LocalDateTime.of(2020, 12, 20, 18, 34)));
    }

    @Test
    public void decimalComparator()
    {
        DfCellComparator cellComparator = df1.columnComparator(df2, "decimal1", "decimal2");
        this.valueEqualsValue(cellComparator.compare(0, 0));
        this.valueAndNull(cellComparator.compare(0, 3));
        this.nullAndValue(cellComparator.compare(2, 0));
        this.bothAreNulls(cellComparator.compare(2, 3));
        this.leftGreaterThanRight(
                cellComparator.compare(1, 0),
                BigDecimal.valueOf(456, 2).subtract(BigDecimal.valueOf(123, 2)).doubleValue());
        this.leftLessThanRight(
                cellComparator.compare(0, 1),
                BigDecimal.valueOf(123, 2).subtract(BigDecimal.valueOf(456, 2)).doubleValue());
    }

    private void valueEqualsValue(ComparisonResult result)
    {
        assertEquals(0, result.compared());
        assertEquals(0.0, result.dDelta());
        assertEquals(0, result.delta());
        assertTrue(result.noNulls());
        assertFalse(result.leftIsNull());
        assertFalse(result.rightIsNull());
        assertFalse(result.bothAreNulls());
    }

    private void valueAndNull(ComparisonResult result)
    {
        assertTrue(result.compared() > 0);
        assertEquals(1.0, result.dDelta());
        assertEquals(1, result.delta());
        assertFalse(result.noNulls());
        assertFalse(result.leftIsNull());
        assertTrue(result.rightIsNull());
        assertFalse(result.bothAreNulls());
    }

    private void nullAndValue(ComparisonResult result)
    {
        assertTrue(result.compared() < 0);
        assertEquals(-1.0, result.dDelta());
        assertEquals(-1, result.delta());
        assertFalse(result.noNulls());
        assertTrue(result.leftIsNull());
        assertFalse(result.rightIsNull());
        assertFalse(result.bothAreNulls());
    }

    private void bothAreNulls(ComparisonResult result)
    {
        assertEquals(0, result.compared());
        assertEquals(0.0, result.dDelta());
        assertEquals(0, result.delta());
        assertFalse(result.noNulls());
        assertTrue(result.bothAreNulls());
        assertTrue(result.leftIsNull());
        assertTrue(result.rightIsNull());
    }

    private void leftGreaterThanRight(ComparisonResult result, long expectedDelta)
    {
        this.leftGreaterThanRight(result, expectedDelta, expectedDelta);
    }

    private void leftGreaterThanRight(ComparisonResult result, double expectedDoubleDelta)
    {
        this.leftGreaterThanRight(result, Math.round(expectedDoubleDelta), expectedDoubleDelta);
    }

    private void leftGreaterThanRight(ComparisonResult result, long expectedDelta, double expectedDoubleDelta)
    {
        assertTrue(result.compared() > 0);
        assertEquals(expectedDelta, result.delta());
        assertEquals(expectedDoubleDelta, result.dDelta(), TOLERANCE);
        assertTrue(result.noNulls());
        assertFalse(result.leftIsNull());
        assertFalse(result.rightIsNull());
        assertFalse(result.bothAreNulls());
    }

    private void leftLessThanRight(ComparisonResult result, long expectedDelta)
    {
        this.leftLessThanRight(result, expectedDelta, expectedDelta);
    }

    private void leftLessThanRight(ComparisonResult result, double expectedDoubleDelta)
    {
        this.leftLessThanRight(result, Math.round(expectedDoubleDelta), expectedDoubleDelta);
    }

    private void leftLessThanRight(ComparisonResult result, long expectedDelta, double expectedDoubleDelta)
    {
        assertTrue(result.compared() < 0);
        assertEquals(expectedDelta, result.delta());
        assertEquals(expectedDoubleDelta, result.dDelta(), TOLERANCE);
        assertTrue(result.noNulls());
        assertFalse(result.leftIsNull());
        assertFalse(result.rightIsNull());
        assertFalse(result.bothAreNulls());
    }
}
