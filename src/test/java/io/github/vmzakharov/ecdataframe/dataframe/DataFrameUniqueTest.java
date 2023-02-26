package io.github.vmzakharov.ecdataframe.dataframe;

import org.eclipse.collections.impl.factory.Lists;
import org.junit.Before;
import org.junit.Test;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;

public class DataFrameUniqueTest
{
    private DataFrame df;

    @Before
    public void setupDataFrame()
    {
        this.df = new DataFrame("Data Frame")
            .addLongColumn("Id").addStringColumn("Name").addDoubleColumn("Foo").addDateColumn("Bar").addDateTimeColumn("Baz").addDecimalColumn("Qux")
            .addRow(3, "Carl", 789.001, LocalDate.of(2022, 11, 15), LocalDateTime.of(2023, 11, 22, 20, 45, 55), BigDecimal.valueOf(456, 2))
            .addRow(1, "Alice", 123.456, LocalDate.of(2022, 12, 15), LocalDateTime.of(2023, 11, 22, 20, 45, 55), BigDecimal.valueOf(123, 2))
            .addRow(2, "Bob", 123.456, LocalDate.of(2022, 12, 15), LocalDateTime.of(2023, 11, 22, 20, 45, 55), BigDecimal.valueOf(123, 2))
            .addRow(1, "Alice", 123.456, LocalDate.of(2022, 12, 15), LocalDateTime.of(2023, 11, 22, 20, 45, 55), BigDecimal.valueOf(123, 2))
            .addRow(1, "Alice", 123.456, LocalDate.of(2022, 12, 15), LocalDateTime.of(2023, 11, 22, 20, 45, 55), BigDecimal.valueOf(123, 2))
            .addRow(2, "Bob", 123.456, LocalDate.of(2022, 11, 15), LocalDateTime.of(2023, 11, 22, 20, 45, 55), BigDecimal.valueOf(456, 2))
            .addRow(3, "Carl", 789.001, LocalDate.of(2022, 11, 15), LocalDateTime.of(2023, 11, 22, 20, 45, 55), BigDecimal.valueOf(456, 2))
            .addRow(1, "Alice", 123.456, LocalDate.of(2022, 12, 15), LocalDateTime.of(2023, 11, 22, 20, 45, 55), BigDecimal.valueOf(567, 1))
            .addRow(3, "Carl", 789.001, LocalDate.of(2022, 11, 15), LocalDateTime.of(2023, 11, 22, 20, 45, 55), BigDecimal.valueOf(456, 2))
            .addRow(1, "Alice", 123.456, LocalDate.of(2022, 12, 15), LocalDateTime.of(2023, 11, 22, 20, 45, 55), BigDecimal.valueOf(123, 2))
            ;
    }

    @Test
    public void allColumns()
    {
        DataFrameUtil.assertEquals(
                new DataFrame("expected")
                        .addLongColumn("Id").addStringColumn("Name").addDoubleColumn("Foo").addDateColumn("Bar").addDateTimeColumn("Baz").addDecimalColumn("Qux")
                        .addRow(3, "Carl", 789.001, LocalDate.of(2022, 11, 15), LocalDateTime.of(2023, 11, 22, 20, 45, 55), BigDecimal.valueOf(456, 2))
                        .addRow(1, "Alice", 123.456, LocalDate.of(2022, 12, 15), LocalDateTime.of(2023, 11, 22, 20, 45, 55), BigDecimal.valueOf(123, 2))
                        .addRow(2, "Bob", 123.456, LocalDate.of(2022, 12, 15), LocalDateTime.of(2023, 11, 22, 20, 45, 55), BigDecimal.valueOf(123, 2))
                        .addRow(2, "Bob", 123.456, LocalDate.of(2022, 11, 15), LocalDateTime.of(2023, 11, 22, 20, 45, 55), BigDecimal.valueOf(456, 2))
                        .addRow(1, "Alice", 123.456, LocalDate.of(2022, 12, 15), LocalDateTime.of(2023, 11, 22, 20, 45, 55), BigDecimal.valueOf(567, 1))
                ,
                this.df.unique()
        );
    }

    @Test
    public void someColumns()
    {
        DataFrameUtil.assertEquals(
                new DataFrame("expected")
                        .addLongColumn("Id").addStringColumn("Name").addDoubleColumn("Foo")
                        .addRow(3, "Carl", 789.001)
                        .addRow(1, "Alice", 123.456)
                        .addRow(2, "Bob", 123.456)
                ,
                this.df.unique(Lists.immutable.of("Id", "Name", "Foo"))
        );
    }

    @Test
    public void someOtherColumns()
    {
        DataFrameUtil.assertEquals(
                new DataFrame("expected")
                    .addDoubleColumn("Foo").addDateColumn("Bar").addDateTimeColumn("Baz").addDecimalColumn("Qux")
                    .addRow(789.001, LocalDate.of(2022, 11, 15), LocalDateTime.of(2023, 11, 22, 20, 45, 55), BigDecimal.valueOf(456, 2))
                    .addRow(123.456, LocalDate.of(2022, 12, 15), LocalDateTime.of(2023, 11, 22, 20, 45, 55), BigDecimal.valueOf(123, 2))
                    .addRow(123.456, LocalDate.of(2022, 11, 15), LocalDateTime.of(2023, 11, 22, 20, 45, 55), BigDecimal.valueOf(456, 2))
                    .addRow(123.456, LocalDate.of(2022, 12, 15), LocalDateTime.of(2023, 11, 22, 20, 45, 55), BigDecimal.valueOf(567, 1))
                ,
                this.df.unique(Lists.immutable.of("Foo", "Bar", "Baz", "Qux"))
        );
    }
}
