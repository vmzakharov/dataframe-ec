package io.github.vmzakharov.ecdataframe.dataframe;

import org.eclipse.collections.impl.factory.Lists;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.LocalDate;

import static io.github.vmzakharov.ecdataframe.dataframe.AggregateFunction.same;

public class DataFrameAggregationSameTest
{
    private DataFrame dataFrame;

    @BeforeEach
    public void initialiseDataFrame()
    {
        this.dataFrame = new DataFrame("FrameOfData")
                .addStringColumn("Name").addStringColumn("Color").addLongColumn("Bar")
                .addDoubleColumn("Baz").addDoubleColumn("Qux").addDateColumn("Date")
                .addIntColumn("Plugh").addFloatColumn("Thud")
                .addRow("Alice", "Blue",    123L, 10.0, 100.0, LocalDate.of(2021, 11, 21), 1, 1.25f)
                .addRow("Bob",   "Orange",  456L, 12.0, -25.0, LocalDate.of(2021, 11, 21), 2, 2.5f)
                .addRow("Bob",   "Orange",  457L, 16.0, -25.0, LocalDate.of(2021, 11, 23), 3, 2.75f)
                .addRow("Carol", "Green",  -789L, 17.0,  42.0, LocalDate.of(2021, 12, 22), 4, 1.125f)
                .addRow("Carol", "Purple", -789L, 17.0,  43.0, LocalDate.of(2021, 12, 22), 4, 1.125f);
    }

    @Test
    public void constantValuesByName()
    {
        DataFrameUtil.assertEquals(
                new DataFrame("aggregated")
                        .addStringColumn("Name").addStringColumn("Color").addLongColumn("Bar").addDoubleColumn("Qux")
                        .addRow("Alice", "Blue",    123L, 100.0)
                        .addRow("Bob",   "Orange",  null, -25.0)
                        .addRow("Carol", null,     -789L,  null),
                this.dataFrame.aggregateBy(
                        Lists.immutable.of(same("Color"), same("Bar"), same("Qux")),
                        Lists.immutable.of("Name")));
    }

    @Test
    public void sameValuesByNameWithDate()
    {
        DataFrameUtil.assertEquals(
                new DataFrame("aggregated")
                        .addStringColumn("Name").addStringColumn("Same Color").addDateColumn("Date")
                        .addRow("Alice", "Blue",   LocalDate.of(2021, 11, 21))
                        .addRow("Bob",   "Orange", null)
                        .addRow("Carol", null,     LocalDate.of(2021, 12, 22)),
                this.dataFrame.aggregateBy(
                        Lists.immutable.of(same("Color", "Same Color"), same("Date")),
                        Lists.immutable.of("Name")));
    }

    @Test
    public void sameValuesByNameWithInt()
    {
        DataFrameUtil.assertEquals(
                new DataFrame("aggregated")
                        .addStringColumn("Name").addStringColumn("Same Color").addIntColumn("Same Plugh")
                        .addRow("Alice", "Blue",   1)
                        .addRow("Bob",   "Orange", null)
                        .addRow("Carol", null,     4),
                this.dataFrame.aggregateBy(
                        Lists.immutable.of(same("Color", "Same Color"), same("Plugh", "Same Plugh")),
                        Lists.immutable.of("Name")));
    }

    @Test
    public void sameValuesByNameWithFloat()
    {
        DataFrameUtil.assertEquals(
                new DataFrame("aggregated")
                        .addStringColumn("Name").addStringColumn("Same Color").addFloatColumn("Same Thud")
                        .addRow("Alice", "Blue",   1.25f)
                        .addRow("Bob",   "Orange", null)
                        .addRow("Carol", null,     1.125f),
                this.dataFrame.aggregateBy(
                        Lists.immutable.of(same("Color", "Same Color"), same("Thud", "Same Thud")),
                        Lists.immutable.of("Name")));
    }

    @Test
    public void aggregateAllNoGrouping()
    {
        DataFrame df = new DataFrame("FrameOfData")
                .addStringColumn("Name").addStringColumn("Color")
                .addLongColumn("Bar").addLongColumn("Baz")
                .addDoubleColumn("Qux").addDoubleColumn("Waldo")
                .addDateColumn("DateOne").addDateColumn("DateTwo")
                .addIntColumn("PlughOne").addIntColumn("PlughTwo")
                .addFloatColumn("ThudOne").addFloatColumn("ThudTwo")
                .addRow("Alice", "Orange", 123L, 123L, 10.0, 100.0, LocalDate.of(2021, 12, 22), LocalDate.of(2021, 11, 21), 4, 2, 1.25f, 2.5f)
                .addRow("Bob",   "Orange", 123L, 456L, 12.0, 100.0, LocalDate.of(2021, 12, 22), LocalDate.of(2021, 11, 22), 4, 1, 1.25f, 2.5f)
                .addRow("Carol", "Orange", 123L, 123L, 17.0, 100.0, LocalDate.of(2021, 12, 22), LocalDate.of(2021, 11, 21), 4, 1, 1.25f, 3.5f)
                ;

        DataFrameUtil.assertEquals(
                new DataFrame("Expected")
                        .addStringColumn("Name").addStringColumn("Color")
                        .addLongColumn("Bar").addLongColumn("Baz")
                        .addDoubleColumn("Qux").addDoubleColumn("Waldo")
                        .addDateColumn("DateOne").addDateColumn("DateTwo")
                        .addIntColumn("PlughOne").addIntColumn("PlughTwo")
                        .addFloatColumn("ThudOne").addFloatColumn("ThudTwo")
                        .addRow(null, "Orange", 123L, null, null, 100.0, LocalDate.of(2021, 12, 22), null, 4, null, 1.25f, null),
                df.aggregate(Lists.immutable.of(
                        same("Name"), same("Color"),
                        same("Bar"), same("Baz"),
                        same("Qux"), same("Waldo"),
                        same("DateOne"), same("DateTwo"),
                        same("PlughOne"), same("PlughTwo"),
                        same("ThudOne"), same("ThudTwo")
                        )
                )
        );
    }
}
