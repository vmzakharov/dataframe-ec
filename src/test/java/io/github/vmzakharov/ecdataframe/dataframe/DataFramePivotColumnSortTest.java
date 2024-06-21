package io.github.vmzakharov.ecdataframe.dataframe;

import org.eclipse.collections.impl.factory.Lists;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.LocalDate;

import static io.github.vmzakharov.ecdataframe.dataframe.AggregateFunction.sum;
import static io.github.vmzakharov.ecdataframe.dataframe.DfColumnSortOrder.ASC;
import static io.github.vmzakharov.ecdataframe.dataframe.DfColumnSortOrder.DESC;

public class DataFramePivotColumnSortTest
{
    private LocalDate day1;
    private LocalDate day2;
    private LocalDate day3;

    private DataFrame donutOrders;

    @BeforeEach
    public void setupDonutOrders()
    {
        this.day1 = LocalDate.of(2024, 10, 15);
        this.day2 = LocalDate.of(2024, 10, 16);
        this.day3 = LocalDate.of(2024, 10, 17);

        this.donutOrders = new DataFrame("Orders")
                .addStringColumn("Customer").addDateColumn("Date").addStringColumn("Donut Type").addLongColumn("Count")
                .addRow("Alice", this.day2, "Blueberry", 2)
                .addRow("Alice", this.day2, "Glazed", 4)
                .addRow("Alice", this.day2, "Old Fashioned", 10)

                .addRow("Carol", this.day1, "Old Fashioned",  1)
                .addRow("Carol", this.day1, "Jelly", 2)
                .addRow("Carol", this.day3, "Old Fashioned", 10)
                .addRow("Carol", this.day3, "Glazed", 1)
                .addRow("Carol", this.day3, "Pumpkin Spice", 12)

                .addRow("Dave",  this.day1, "Pumpkin Spice", 2)
                .addRow("Dave",  this.day2, "Old Fashioned", 12)
                .addRow("Dave",  this.day3, "Old Fashioned", 10)

                .addRow("Bob",   this.day1, "Blueberry", 12)
                .addRow("Bob",   this.day2, "Blueberry", 12)
                ;
    }

    @Test
    public void pivotWithUnsortedColumns()
    {
        DataFrame customerByDateSumQty = this.donutOrders
                        .pivot(Lists.immutable.of("Customer"),
                                "Date",
                                Lists.immutable.of(sum("Count"))
                        );

        DataFrameUtil.assertEquals(
                new DataFrame("expected")
                        .addStringColumn("Customer")
                        .addLongColumn(this.day2.toString())
                        .addLongColumn(this.day1.toString())
                        .addLongColumn(this.day3.toString())
                        .addRow("Alice", 16,  0,  0)
                        .addRow("Carol",  0,  3, 23)
                        .addRow("Dave",  12,  2, 10)
                        .addRow("Bob",   12, 12,  0),
                customerByDateSumQty
        );
    }

    @Test
    public void pivotWithColumnsSortedAsc()
    {
        DataFrame customerByDateSumQty = this.donutOrders
                        .pivot(Lists.immutable.of("Customer"),
                                "Date", ASC,
                                Lists.immutable.of(sum("Count"))
                        );

        DataFrameUtil.assertEquals(
                new DataFrame("expected")
                        .addStringColumn("Customer")
                        .addLongColumn(this.day1.toString())
                        .addLongColumn(this.day2.toString())
                        .addLongColumn(this.day3.toString())
                        .addRow("Alice",  0, 16,  0)
                        .addRow("Carol",  3,  0, 23)
                        .addRow("Dave",   2, 12, 10)
                        .addRow("Bob",   12, 12,  0),
                customerByDateSumQty
        );
    }

    @Test
    public void pivotWithColumnsSortedDesc()
    {
        DataFrame customerByDateSumQty = this.donutOrders
                        .pivot(Lists.immutable.of("Customer"),
                                "Date", DESC,
                                Lists.immutable.of(sum("Count"))
                        );

        DataFrameUtil.assertEquals(
                new DataFrame("expected")
                        .addStringColumn("Customer")
                        .addLongColumn(this.day3.toString())
                        .addLongColumn(this.day2.toString())
                        .addLongColumn(this.day1.toString())
                        .addRow("Alice",  0,  16,  0)
                        .addRow("Carol", 23,   0,  3)
                        .addRow("Dave",  10,  12,  2)
                        .addRow("Bob",    0,  12, 12),
                customerByDateSumQty
        );
    }

    @Test
    public void pivotNamesWithColumnsSorted()
    {
        DataFrame dateByCustomerSumQty = this.donutOrders
                .pivot(Lists.immutable.of("Date"),
                        "Customer", DESC,
                        Lists.immutable.of(sum("Count"))
                );

        dateByCustomerSumQty.sortBy(Lists.immutable.of("Date"));

        DataFrameUtil.assertEquals(
                new DataFrame("expected")
                        .addDateColumn("Date")
                        .addLongColumn("Dave")
                        .addLongColumn("Carol")
                        .addLongColumn("Bob")
                        .addLongColumn("Alice")
                        .addRow(this.day1,  2,  3, 12,  0)
                        .addRow(this.day2, 12,  0, 12, 16)
                        .addRow(this.day3, 10, 23,  0,  0),
                dateByCustomerSumQty
        );
    }

}
