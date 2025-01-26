package io.github.vmzakharov.ecdataframe.dataframe;

import org.eclipse.collections.api.factory.Lists;
import org.eclipse.collections.api.list.MutableList;
import org.eclipse.collections.api.set.MutableSet;
import org.eclipse.collections.impl.block.factory.HashingStrategies;
import org.eclipse.collections.impl.factory.HashingStrategySets;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.time.LocalDate;

public class DataFramePoolingTest
{
    private final MutableList<String> nameRoots = Lists.mutable.of("Alice", "Bob", "Carol", "Dave", "Eve");

    @Test
    public void poolingDisabled()
    {
        DataFrame df = this.createDataFrame();

        this.populateDataFrame(df, 0);
        this.populateDataFrame(df, 1);

        MutableSet<Object> names = this.createIdentitySet().withAll(df.getStringColumn("name").toList());
        MutableSet<Object> dates = this.createIdentitySet().withAll(df.getDateColumn("date").toList());

        Assertions.assertEquals(10, names.size());
        Assertions.assertEquals(10, dates.size());
    }

    @Test
    public void poolingEnabled()
    {
        DataFrame df = this.createDataFrame();

        df.enablePooling();

        this.populateDataFrame(df, 0);
        this.populateDataFrame(df, 1);

        MutableSet<Object> names = this.createIdentitySet().withAll(df.getStringColumn("name").toList());
        MutableSet<Object> dates = this.createIdentitySet().withAll(df.getDateColumn("date").toList());

        Assertions.assertEquals(5, names.size());
        Assertions.assertEquals(5, dates.size());
    }

    private DataFrame createDataFrame()
    {
        return new DataFrame("df")
                .addStringColumn("name").addDateColumn("date").addIntColumn("number");
    }

    private void populateDataFrame(DataFrame dataFrame, int offset)
    {
        this.nameRoots.forEachWithIndex(
            (name, index) -> dataFrame.addRow(name + index, LocalDate.of(2025, 10, index + 1), offset * 10 + index)
        );
    }

    private MutableSet<Object> createIdentitySet()
    {
        return HashingStrategySets.mutable.with(HashingStrategies.identityStrategy());
    }
}
