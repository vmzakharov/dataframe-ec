package io.github.vmzakharov.ecdataframe.dataframe;

import io.github.vmzakharov.ecdataframe.dataframe.compare.ComparisonResult;
import org.eclipse.collections.impl.factory.Lists;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;

import static org.junit.jupiter.api.Assertions.*;

public class DataFrameColumnComparatorTest
{
    public static final double TOLERANCE = 0.00000001;

    private DataFrame df1;
    private DataFrame df2;

    @BeforeEach
    public void initializeDataFrames()
    {
        this.df1 = new DataFrame("df1")
            .addStringColumn("string1").addStringColumn("string1_a")
            .addLongColumn("long1").addIntColumn("int1")
            .addDoubleColumn("double1").addDoubleColumn("double1_a").addFloatColumn("float1")
            .addBooleanColumn("boolean1")
            .addDateColumn("date1").addDateTimeColumn("dateTime1")
            .addDecimalColumn("decimal1")
            .addRow("a",  "a",  10L, null, 13.25, 13.25, 41.0f,  null,  LocalDate.of(2020, 10, 20),                                   null, BigDecimal.valueOf(123, 2))
            .addRow("b",  "b", null,   22, 23.25, 23.25,  null, false,                        null, LocalDateTime.of(2020, 10, 20, 16, 32), BigDecimal.valueOf(456, 2))
            .addRow(null, "c",  30L,   23,  null, 33.25, 43.0f, false,  LocalDate.of(2020, 10, 24), LocalDateTime.of(2020, 11, 20, 17, 33),                       null)
            .addRow("d",  "d",  40L,   24, 43.25, 43.25, 44.5f,  true,  LocalDate.of(2020, 10, 26), LocalDateTime.of(2020, 12, 20, 18, 34), BigDecimal.valueOf(901, 2))
            ;

        this.df2 = new DataFrame("df2")
            .addStringColumn("string2").addStringColumn("string2_a")
            .addLongColumn("long2").addIntColumn("int2")
            .addDoubleColumn("double2").addFloatColumn("float2")
            .addBooleanColumn("boolean2")
            .addDateColumn("date2").addDateTimeColumn("dateTime2")
            .addDecimalColumn("decimal2")
            .addRow("a",  "d",  10L,   21, 13.25, 41.0f,  true,  LocalDate.of(2020, 10, 20), LocalDateTime.of(2020,  9, 20, 15, 31), BigDecimal.valueOf(123, 2))
            .addRow(null, "a",  20L,   22,  null, 42.0f, false,                        null, LocalDateTime.of(2020, 10, 20, 16, 32), BigDecimal.valueOf(456, 2))
            .addRow("c",  "c", null,   23, 33.50,  null, false,  LocalDate.of(2020, 10, 24),                                   null, BigDecimal.valueOf(789, 2))
            .addRow("d",  "b",  40L, null, 43.25, 44.5f,  null,  LocalDate.of(2020, 10, 26), LocalDateTime.of(2020, 12, 20, 18, 34),                       null)
            ;
    }

    @Test
    public void stringComparator()
    {
        DfCellComparator cellComparator = this.df1.columnComparator(this.df2, "string1", "string2");
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
        DfCellComparator cellComparator = this.df1.columnComparator(this.df2, "long1", "long2");
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
        DfCellComparator cellComparator = this.df1.columnComparator(this.df2, "int1", "int2");
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
        DfCellComparator cellComparator = this.df1.columnComparator(this.df2, "double1", "double2");
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
        DfCellComparator cellComparator = this.df1.columnComparator(this.df2, "float1", "float2");
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
        DfCellComparator cellComparator = this.df1.columnComparator(this.df2, "boolean1", "boolean2");
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
        DfCellComparator cellComparator = this.df1.columnComparator(this.df2, "date1", "date2");
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
        DfCellComparator cellComparator = this.df1.columnComparator(this.df2, "dateTime1", "dateTime2");
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
        DfCellComparator cellComparator = this.df1.columnComparator(this.df2, "decimal1", "decimal2");
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

    @Test
    public void compareWithOverflow()
    {
        DataFrame dataFrame1 = new DataFrame("DF1")
                .addLongColumn("order").addLongColumn("col1").addDoubleColumn("col2")
                .addRow(1, Long.MAX_VALUE, Double.MAX_VALUE)
                .addRow(2, Long.MAX_VALUE, Double.MAX_VALUE)
                .addRow(3, Long.MAX_VALUE, Double.MAX_VALUE)
                .addRow(4, Long.MIN_VALUE, -Double.MAX_VALUE)
                .addRow(5, Long.MIN_VALUE, -Double.MAX_VALUE)
                .addRow(6, (Long.MAX_VALUE / 3) * 2, (Double.MAX_VALUE / 3.0) * 2.0)
                .addRow(7, (Long.MIN_VALUE / 4) * 3, (-Double.MAX_VALUE / 4.0) * 3.0)
                ;

        DataFrame dataFrame2 = new DataFrame("DF2")
                .addLongColumn("order").addLongColumn("number").addDoubleColumn("value")
                .addRow(1, Long.MAX_VALUE, Double.MAX_VALUE)
                .addRow(2,  -1,  -1.0)
                .addRow(3, 100, 100.0)
                .addRow(4,  10,  10.0)
                .addRow(5,  -1,  -1.0)
                .addRow(6, (Long.MIN_VALUE / 3) * 2, (-Double.MAX_VALUE / 3.0) * 2.0)
                .addRow(7, (Long.MAX_VALUE / 4) * 3, (Double.MAX_VALUE / 4.0) * 3.0)
                ;

        DfCellComparator col1Comparator = dataFrame1.columnComparator(dataFrame2, "col1", "number");

        this.valueEqualsValue(col1Comparator.compare(0, 0));
        this.leftGreaterThanRight(col1Comparator.compare(1, 1), Long.MAX_VALUE); // correcting for overflow
        this.leftGreaterThanRight(col1Comparator.compare(2, 2), Long.MAX_VALUE - 100);
        this.leftLessThanRight(col1Comparator.compare(3, 3), Long.MIN_VALUE); // correcting for overflow
        this.leftLessThanRight(col1Comparator.compare(4, 4), Long.MIN_VALUE - (-1));
        this.leftGreaterThanRight(col1Comparator.compare(5, 5), Long.MAX_VALUE); // correcting for overflow
        this.leftLessThanRight(col1Comparator.compare(6, 6), Long.MIN_VALUE); // correcting for overflow

        DfCellComparator col2Comparator = dataFrame1.columnComparator(dataFrame2, "col2", "value");

        this.valueEqualsValue(col2Comparator.compare(0, 0));
        this.leftGreaterThanRight(col2Comparator.compare(1, 1), Double.MAX_VALUE);
        this.leftGreaterThanRight(col2Comparator.compare(2, 2), Double.MAX_VALUE - 100);
        this.leftLessThanRight(col2Comparator.compare(3, 3), -Double.MAX_VALUE);
        this.leftLessThanRight(col2Comparator.compare(4, 4), -Double.MAX_VALUE);
        this.leftGreaterThanRight(col2Comparator.compare(5, 5), Double.POSITIVE_INFINITY);
        this.leftLessThanRight(col2Comparator.compare(6, 6), Double.NEGATIVE_INFINITY);
        assertTrue(col2Comparator.compare(6, 6).compared() < 0);

        assertTrue(Double.isInfinite(col2Comparator.compare(5, 5).dDelta()));
        assertTrue(col2Comparator.compare(5, 5).dDelta() > 0);

        assertTrue(Double.isInfinite(col2Comparator.compare(6, 6).dDelta()));
        assertTrue(col2Comparator.compare(6, 6).dDelta() < 0);
    }

    @Test
    public void compareWithSameDataFrame()
    {
        DfCellComparator coCo1 = this.df1.columnComparator(this.df1, "string1", "string1");

        this.valueEqualsValue(coCo1.compare(0, 0));
        this.nullAndValue(coCo1.compare(2, 1));
        this.leftLessThanRight(coCo1.compare(0, 1), "a".compareTo("b"));
        this.leftGreaterThanRight(coCo1.compare(1, 0), "b".compareTo("a"));
        this.valueAndNull(coCo1.compare(1, 2));

        DfCellComparator coCo2 = this.df1.columnComparator(this.df1, "double1", "double1_a");

        this.valueEqualsValue(coCo2.compare(0, 0));
        this.valueEqualsValue(coCo2.compare(1, 1));
        this.nullAndValue(coCo2.compare(2, 2));
        this.valueEqualsValue(coCo2.compare(3, 3));

        this.leftLessThanRight(coCo2.compare(0, 1), 13.25 - 23.25);
        this.leftGreaterThanRight(coCo2.compare(1, 0), 23.25 - 13.25);
    }

    @Test
    public void compareSortedDataFrames()
    {
        this.df1.sortBy(
            Lists.immutable.of("string1"),
            Lists.immutable.of(DfColumnSortOrder.DESC));

        this.df2.sortBy(Lists.immutable.of("string2_a"));

        DfCellComparator coCo1 = this.df1.columnComparator(this.df2, "string1", "string2_a");

        this.leftGreaterThanRight(coCo1.compare(0, 0), "d".compareTo("a"));
        this.valueEqualsValue(coCo1.compare(1, 1));
        this.leftLessThanRight(coCo1.compare(2, 2), "a".compareTo("c"));
        this.nullAndValue(coCo1.compare(3, 3));

        DfCellComparator coCo2 = this.df1.columnComparator(this.df2, "double1_a", "double2");

        this.valueAndNull(coCo2.compare(0, 0));
        this.leftLessThanRight(coCo2.compare(1, 1), 23.25 - 43.25);
        this.leftLessThanRight(coCo2.compare(2, 2), 13.25 - 33.50);
        this.leftGreaterThanRight(coCo2.compare(3, 3), 33.25 - 13.25);
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
