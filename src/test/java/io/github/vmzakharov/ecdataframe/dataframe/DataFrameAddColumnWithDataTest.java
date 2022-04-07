package io.github.vmzakharov.ecdataframe.dataframe;

import org.eclipse.collections.impl.factory.Lists;
import org.eclipse.collections.impl.factory.primitive.DoubleLists;
import org.eclipse.collections.impl.factory.primitive.LongLists;
import org.junit.Test;

import java.time.LocalDate;
import java.time.LocalDateTime;

public class DataFrameAddColumnWithDataTest
{
    @Test
    public void addColumnsWithData()
    {
        DataFrame df = new DataFrame("Frame")
                .addStringColumn("Foo", Lists.immutable.of("A", "B", "C"))
                .addLongColumn("Bar", LongLists.immutable.of(1, 2, 3))
                .addDoubleColumn("Baz", DoubleLists.immutable.of(1.1, 2.2, 3.3))
                .addDateColumn("Qux", Lists.immutable.of(LocalDate.of(2020, 10, 21), LocalDate.of(2020, 11, 22), LocalDate.of(2020, 12, 23)))
                .addDateTimeColumn("Quux", Lists.immutable.of(
                        LocalDateTime.of(2020, 10, 21, 10, 10, 10),
                        LocalDateTime.of(2020, 11, 22, 11, 11, 11),
                        LocalDateTime.of(2020, 12, 23, 12, 12, 12)))
                ;

        df.seal();

        DataFrame expected = new DataFrame("Frame")
                .addStringColumn("Foo").addLongColumn("Bar").addDoubleColumn("Baz").addDateColumn("Qux").addDateTimeColumn("Quux")
                .addRow("A", 1, 1.1, LocalDate.of(2020, 10, 21), LocalDateTime.of(2020, 10, 21, 10, 10, 10))
                .addRow("B", 2, 2.2, LocalDate.of(2020, 11, 22), LocalDateTime.of(2020, 11, 22, 11, 11, 11))
                .addRow("C", 3, 3.3, LocalDate.of(2020, 12, 23), LocalDateTime.of(2020, 12, 23, 12, 12, 12))
                ;

        DataFrameUtil.assertEquals(expected, df);
    }

    @Test
    public void addRowsThenColumnsWithData()
    {
        DataFrame df = new DataFrame("Frame")
                .addStringColumn("Foo").addLongColumn("Bar")
                .addRow("A", 1)
                .addRow("B", 2)
                .addRow("C", 3)
                .addDoubleColumn("Baz", DoubleLists.immutable.of(1.1, 2.2, 3.3))
                .addDateColumn("Qux", Lists.immutable.of(LocalDate.of(2020, 10, 21), LocalDate.of(2020, 11, 22), LocalDate.of(2020, 12, 23)))
                .addStringColumn("Waldo", Lists.immutable.of("Bee", "Boo", "Baa"))
                ;

        df.seal();

        DataFrame expected = new DataFrame("Frame")
                .addStringColumn("Foo").addLongColumn("Bar").addDoubleColumn("Baz").addDateColumn("Qux").addStringColumn("Waldo")
                .addRow("A", 1, 1.1, LocalDate.of(2020, 10, 21), "Bee")
                .addRow("B", 2, 2.2, LocalDate.of(2020, 11, 22), "Boo")
                .addRow("C", 3, 3.3, LocalDate.of(2020, 12, 23), "Baa")
                ;

        DataFrameUtil.assertEquals(expected, df);
    }

    @Test
    public void addColumnsWithDataThenRows()
    {
        DataFrame df = new DataFrame("Frame")
                .addStringColumn("Foo", Lists.immutable.of("A", "B"))
                .addLongColumn("Bar", LongLists.immutable.of(1, 2))
                .addDoubleColumn("Baz", DoubleLists.immutable.of(1.1, 2.2))
                .addDateColumn("Qux", Lists.immutable.of(LocalDate.of(2020, 10, 21), LocalDate.of(2020, 11, 22)))
                .addRow("C", 3, 3.3, LocalDate.of(2020, 12, 23))
                .addRow("D", 4, 4.4, LocalDate.of(2020, 12, 25))
                ;

        df.seal();

        DataFrame expected = new DataFrame("Frame")
                .addStringColumn("Foo").addLongColumn("Bar").addDoubleColumn("Baz").addDateColumn("Qux")
                .addRow("A", 1, 1.1, LocalDate.of(2020, 10, 21))
                .addRow("B", 2, 2.2, LocalDate.of(2020, 11, 22))
                .addRow("C", 3, 3.3, LocalDate.of(2020, 12, 23))
                .addRow("D", 4, 4.4, LocalDate.of(2020, 12, 25))
                ;

        DataFrameUtil.assertEquals(expected, df);
    }
}
