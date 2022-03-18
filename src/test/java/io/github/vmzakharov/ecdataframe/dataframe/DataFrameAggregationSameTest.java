package io.github.vmzakharov.ecdataframe.dataframe;

import org.eclipse.collections.impl.factory.Lists;
import org.junit.Before;
import org.junit.Test;

import java.time.LocalDate;

import static io.github.vmzakharov.ecdataframe.dataframe.AggregateFunction.same;

public class DataFrameAggregationSameTest
{
    private DataFrame dataFrame;

    @Before
    public void initialiseDataFrame()
    {
        this.dataFrame = new DataFrame("FrameOfData")
                .addStringColumn("Name").addStringColumn("Color").addLongColumn("Bar")
                .addDoubleColumn("Baz").addDoubleColumn("Qux").addDateColumn("Date")
                .addRow("Alice", "Blue",    123L, 10.0, 100.0, LocalDate.of(2021, 11, 21))
                .addRow("Bob",   "Orange",  456L, 12.0, -25.0, LocalDate.of(2021, 11, 21))
                .addRow("Bob",   "Orange",  457L, 16.0, -25.0, LocalDate.of(2021, 11, 23))
                .addRow("Carol", "Green",  -789L, 17.0,  42.0, LocalDate.of(2021, 12, 22))
                .addRow("Carol", "Purple", -789L, 17.0,  43.0, LocalDate.of(2021, 12, 22));
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
    public void constantValuesByNameWithDate()
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
    public void constantValues()
    {
        DataFrame df = new DataFrame("FrameOfData")
                .addStringColumn("Name").addStringColumn("Color").addLongColumn("Bar").addDoubleColumn("Baz")
                .addDoubleColumn("Qux").addDoubleColumn("Waldo").addDateColumn("DateOne").addDateColumn("DateTwo")
                .addRow("Alice", "Orange", 123L, 123L, 10.0, 100.0, LocalDate.of(2021, 12, 22), LocalDate.of(2021, 11, 21))
                .addRow("Bob",   "Orange", 123L, 456L, 12.0, 100.0, LocalDate.of(2021, 12, 22), LocalDate.of(2021, 11, 22))
                .addRow("Carol", "Orange", 123L, 123L, 17.0, 100.0, LocalDate.of(2021, 12, 22), LocalDate.of(2021, 11, 21));

        DataFrameUtil.assertEquals(
                new DataFrame("Expected")
                        .addStringColumn("Name").addStringColumn("Color").addLongColumn("Bar").addDoubleColumn("Baz")
                        .addDoubleColumn("Qux").addDoubleColumn("Waldo").addDateColumn("DateOne").addDateColumn("DateTwo")
                        .addRow(null, "Orange", 123L, null, null, 100.0, LocalDate.of(2021, 12, 22), null),
                df.aggregate(Lists.immutable.of(
                        same("Name"), same("Color"), same("Bar"), same("Baz"),
                        same("Qux"), same("Waldo"), same("DateOne"), same("DateTwo"))
                )
        );
    }
}
