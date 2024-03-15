package io.github.vmzakharov.ecdataframe.dataframe;

import org.eclipse.collections.api.bag.MutableBag;
import org.eclipse.collections.api.factory.Bags;
import org.eclipse.collections.api.factory.Lists;
import org.eclipse.collections.api.list.ListIterable;
import org.eclipse.collections.api.list.primitive.DoubleList;
import org.eclipse.collections.impl.factory.primitive.DoubleLists;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;

import static io.github.vmzakharov.ecdataframe.dataframe.DfColumnSortOrder.DESC;

public class DataFrameIterateTest
{
    private DataFrame dataFrame;

    @Before
    public void setUpDataFrame()
    {
        this.dataFrame = new DataFrame("FrameOfData")
            .addStringColumn("Name").addLongColumn("Bar").addDoubleColumn("Baz")
            .addDateColumn("Date").addDateTimeColumn("DateTime").addDecimalColumn("Decimal").addIntColumn("Qux")
            .addRow("Alice",   11L, 20.0, LocalDate.of(2023, 10, 21), LocalDateTime.of(2023, 10, 21, 11, 11, 11), BigDecimal.valueOf(123, 2), 110)
            .addRow("Carol",   12L, 10.0, LocalDate.of(2023, 12, 24), LocalDateTime.of(2023, 10, 21, 11, 11, 11), BigDecimal.valueOf(124, 2), 120)
            .addRow("Alice",   33L, 25.0, LocalDate.of(2023, 11, 23), LocalDateTime.of(2023, 11, 23, 11, 13, 11), BigDecimal.valueOf(123, 2), 330)
            .addRow("Carol",   24L, 66.1, LocalDate.of(2023, 12, 24), LocalDateTime.of(2023, 12, 24, 11, 14, 11), BigDecimal.valueOf(222, 2), 240)
            .addRow("Carol",   24L, 41.0, LocalDate.of(2023, 12, 24), LocalDateTime.of(2023, 12, 24, 11, 15, 11), BigDecimal.valueOf(333, 2), 240)
            .addRow("Abigail", 33L, 11.0, LocalDate.of(2023, 11, 23), LocalDateTime.of(2023, 10, 21, 11, 11, 11), BigDecimal.valueOf(123, 2), 330)
        ;
    }

    @Test
    public void dataFrameForEach()
    {
        StringBuilder nameBarBaz = new StringBuilder();

        this.dataFrame.forEach(c ->
            nameBarBaz.append(c.getString("Name")).append(',')
                      .append(c.getLong("Bar")).append(',')
                      .append(c.getDouble("Baz")).append(',')
                      .append(c.getInt("Qux")).append('\n')
        );

        Assert.assertEquals(
            "Alice,11,20.0,110\n"
            + "Carol,12,10.0,120\n"
            + "Alice,33,25.0,330\n"
            + "Carol,24,66.1,240\n"
            + "Carol,24,41.0,240\n"
            + "Abigail,33,11.0,330\n",
            nameBarBaz.toString()
        );

        MutableBag<LocalDate> dateCount = Bags.mutable.of();

        this.dataFrame.forEach(c -> dateCount.add(c.getDate("Date")));

        Assert.assertEquals(1, dateCount.occurrencesOf(LocalDate.of(2023, 10, 21)));
        Assert.assertEquals(3, dateCount.occurrencesOf(LocalDate.of(2023, 12, 24)));
        Assert.assertEquals(2, dateCount.occurrencesOf(LocalDate.of(2023, 11, 23)));

        MutableBag<LocalDateTime> dateTimeCount = Bags.mutable.of();

        this.dataFrame.forEach(c -> dateTimeCount.add(c.getDateTime("DateTime")));

        Assert.assertEquals(3, dateTimeCount.occurrencesOf(LocalDateTime.of(2023, 10, 21, 11, 11, 11)));
        Assert.assertEquals(1, dateTimeCount.occurrencesOf(LocalDateTime.of(2023, 11, 23, 11, 13, 11)));
        Assert.assertEquals(1, dateTimeCount.occurrencesOf(LocalDateTime.of(2023, 12, 24, 11, 14, 11)));
        Assert.assertEquals(1, dateTimeCount.occurrencesOf(LocalDateTime.of(2023, 12, 24, 11, 15, 11)));

        MutableBag<BigDecimal> decimalCount = Bags.mutable.of();

        this.dataFrame.forEach(c -> decimalCount.add(c.getDecimal("Decimal")));

        Assert.assertEquals(3, decimalCount.occurrencesOf(BigDecimal.valueOf(123, 2)));
        Assert.assertEquals(1, decimalCount.occurrencesOf(BigDecimal.valueOf(124, 2)));
        Assert.assertEquals(1, decimalCount.occurrencesOf(BigDecimal.valueOf(222, 2)));
        Assert.assertEquals(1, decimalCount.occurrencesOf(BigDecimal.valueOf(333, 2)));
    }

    @Test
    public void sortedDataFrameForEach()
    {
        StringBuilder nameBarBaz = new StringBuilder();

        this.dataFrame.sortBy(Lists.immutable.of("Name", "Bar"), Lists.immutable.of(DfColumnSortOrder.ASC, DESC));

        this.dataFrame.forEach(c ->
            nameBarBaz.append(c.getString("Name")).append(',')
                      .append(c.getLong("Bar")).append(',')
                      .append(c.getDouble("Baz")).append(',')
                      .append(c.getInt("Qux")).append('\n')
        );

       Assert.assertEquals(
            "Abigail,33,11.0,330\n"
            + "Alice,33,25.0,330\n"
            + "Alice,11,20.0,110\n"
            + "Carol,24,66.1,240\n"
            + "Carol,24,41.0,240\n"
            + "Carol,12,10.0,120\n"
            ,
            nameBarBaz.toString()
        );
    }

    @Test
    public void emptyDataFrameForEach()
    {
        this.dataFrame.cloneStructure("Empty").forEach(c -> Assert.fail());
    }

    @Test
    public void indexIterate()
    {
        this.dataFrame.createIndex("anIndex", Lists.immutable.of("Name", "Bar"));

        Assert.assertEquals(2, this.dataFrame.index("anIndex").sizeAt("Carol", 24L));

        ListIterable<BigDecimal> expectedLong = Lists.immutable.of(BigDecimal.valueOf(222, 2), BigDecimal.valueOf(333, 2));

        this.dataFrame.index("anIndex")
                 .iterateAt("Carol", 24L)
                 .forEach(c -> Assert.assertTrue(expectedLong.contains(c.getDecimal("Decimal"))));

        DoubleList expectedDouble = DoubleLists.immutable.of(66.1, 41.0);

        this.dataFrame.index("anIndex")
                 .iterateAt("Carol", 24L)
                 .forEach(c -> Assert.assertTrue(expectedDouble.contains(c.getDouble("Baz"))));

        ListIterable<LocalDate> expectedDates = Lists.immutable.of(LocalDate.of(2023, 12, 24));

        this.dataFrame.index("anIndex")
                 .iterateAt("Carol", 24L)
                 .forEach(c -> Assert.assertTrue(expectedDates.contains(c.getDate("Date"))));

        ListIterable<LocalDateTime> expectedDateTimes =
                Lists.immutable.of(LocalDateTime.of(2023, 12, 24, 11, 14, 11), LocalDateTime.of(2023, 12, 24, 11, 15, 11));

        this.dataFrame.index("anIndex")
                 .iterateAt("Carol", 24L)
                 .forEach(
                         c -> Assert.assertTrue(expectedDateTimes.contains(c.getDateTime("DateTime")))
                 );

        this.dataFrame.index("anIndex")
                 .iterateAt("Dave", 48L)
                 .forEach(c -> Assert.fail());
    }
}
