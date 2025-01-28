package io.github.vmzakharov.ecdataframe.dataframe;

import org.eclipse.collections.api.factory.Lists;
import org.eclipse.collections.api.list.MutableList;
import org.eclipse.collections.api.set.MutableSet;
import org.eclipse.collections.api.tuple.Twin;
import org.eclipse.collections.impl.block.factory.HashingStrategies;
import org.eclipse.collections.impl.factory.HashingStrategySets;
import org.junit.jupiter.api.Test;

import java.time.LocalDate;

import static org.junit.jupiter.api.Assertions.*;

public class DataFramePoolingTest
{
    private final MutableList<String> nameRoots = Lists.mutable.of("Alice", "Bob", "Carol", "Dave", "Eve");

    @Test
    public void poolingDisabled()
    {
        DataFrame df = this.createDataFrame();

        this.populateDataFrame(df, 0);
        this.populateDataFrame(df, 1);

        this.assertInstanceCount(df, 10);
    }

    @Test
    public void poolingEnabled()
    {
        DataFrame df = this.createDataFrame();
        df.enablePooling();

        this.populateDataFrame(df, 0);
        this.populateDataFrame(df, 1);

        this.assertInstanceCount(df, 5);
    }

    @Test
    public void enablingPoolingCompressesData()
    {
        DataFrame df = this.createDataFrame();
        this.populateDataFrame(df, 0);
        this.populateDataFrame(df, 1);

        this.assertInstanceCount(df, 10);

        df.enablePooling();
        this.assertInstanceCount(df, 5);
    }

    @Test
    public void selectedWithPooling()
    {
        DataFrame source = this.createDataFrame();
        source.enablePooling();

        this.populateDataFrame(source, 0);
        this.populateDataFrame(source, 1);

        this.assertInstanceCount(source, 5);

        DataFrame filtered = source.selectBy("startsWith(name, 'Bob')");

        assertTrue(filtered.isPoolingEnabled());

        this.populateDataFrame(filtered, 2);

        this.assertInstanceCount(filtered, 5);

        DataFrame bobsOnly = filtered.selectBy("startsWith(name, 'Bob')");
        assertEquals(3, bobsOnly.rowCount());

        this.assertInstanceCount(bobsOnly, 1);
    }

    @Test
    public void rejectedWithPooling()
    {
        DataFrame source = this.createDataFrame();
        source.enablePooling();

        this.populateDataFrame(source, 0);
        this.populateDataFrame(source, 1);

        this.assertInstanceCount(source, 5);

        DataFrame filtered = source.rejectBy("startsWith(name, 'Bob')");
        assertTrue(filtered.isPoolingEnabled());

        this.populateDataFrame(filtered, 2);
        this.assertInstanceCount(filtered, 5);

        DataFrame noBobsHere = filtered.rejectBy("startsWith(name, 'Bob')");
        assertEquals(12, noBobsHere.rowCount());

        this.assertInstanceCount(noBobsHere, 4);
    }

    @Test
    public void rejectedWithoutPooling()
    {
        DataFrame source = this.createDataFrame();
        source.enablePooling();

        this.populateDataFrame(source, 0);
        this.populateDataFrame(source, 1);

        this.assertInstanceCount(source, 5);
        source.disablePooling();

        DataFrame filtered = source.rejectBy("startsWith(name, 'Bob')");
        assertFalse(filtered.isPoolingEnabled());

        this.populateDataFrame(filtered, 2);
        this.assertInstanceCount(filtered, 9);

        DataFrame noBobsHere = filtered.rejectBy("startsWith(name, 'Bob')");
        assertEquals(12, noBobsHere.rowCount());

        this.assertInstanceCount(noBobsHere, 8);
    }

    @Test
    public void partitionWithPooling()
    {
        DataFrame source = this.createDataFrame();
        source.enablePooling();

        this.populateDataFrame(source, 0);
        this.populateDataFrame(source, 1);

        this.assertInstanceCount(source, 5);

        Twin<DataFrame> partition = source.partition("startsWith(name, 'Bob')");
        DataFrame bobs = partition.getOne();
        DataFrame notBobs = partition.getTwo();
        assertTrue(bobs.isPoolingEnabled());
        assertTrue(notBobs.isPoolingEnabled());

        bobs.addRow("Bob1", LocalDate.of(2025, 10, 2), 47);
        this.assertInstanceCount(bobs, 1);

        this.assertInstanceCount(notBobs, 4);
        this.populateDataFrame(notBobs, 3);
        this.assertInstanceCount(notBobs, 5);
    }

    @Test
    public void partitionWhenPoolingDisabled()
    {
        DataFrame source = this.createDataFrame();
        source.enablePooling();

        this.populateDataFrame(source, 0);
        this.populateDataFrame(source, 1);

        this.assertInstanceCount(source, 5);
        source.disablePooling();

        Twin<DataFrame> partition = source.partition("startsWith(name, 'Bob')");
        DataFrame bobs = partition.getOne();
        DataFrame notBobs = partition.getTwo();
        assertFalse(bobs.isPoolingEnabled());
        assertFalse(notBobs.isPoolingEnabled());

        bobs.addRow("Bob1", LocalDate.of(2025, 10, 2), 47);
        this.assertInstanceCount(bobs, 2);

        this.assertInstanceCount(notBobs, 4);
        this.populateDataFrame(notBobs, 3);
        this.assertInstanceCount(notBobs, 9);
    }

    @Test
    public void unionWithPooling()
    {
        DataFrame df1 = this.createDataFrame();
        df1.enablePooling();
        this.populateDataFrame(df1, 0);

        DataFrame df2 = this.createDataFrame();
        df2.enablePooling();
        this.populateDataFrame(df2, 1);

        DataFrame unioned = df1.union(df2);
        this.assertInstanceCount(unioned, 5);
    }

    @Test
    public void unionWithoutPooling()
    {
        DataFrame df1 = this.createDataFrame();
        this.populateDataFrame(df1, 0);

        DataFrame df2 = this.createDataFrame();
        this.populateDataFrame(df2, 1);

        DataFrame unioned = df1.union(df2);
        this.assertInstanceCount(unioned, 10);
    }

    @Test
    public void filterCheckedWithPooling()
    {
        DataFrame dataFrame = this.createDataFrame();
        dataFrame.enablePooling();
        this.populateDataFrame(dataFrame, 0);
        this.populateDataFrame(dataFrame, 1);

        this.assertInstanceCount(dataFrame, 5);
        assertEquals("Carol2", dataFrame.getString("name", 2));
        assertEquals("Carol2", dataFrame.getString("name", 7));
        dataFrame.setFlag(2);
        dataFrame.setFlag(7);

        DataFrame flagged = dataFrame.selectFlagged();
        this.assertInstanceCount(flagged, 1); // all are Carols
        assertTrue(flagged.isPoolingEnabled());

        DataFrame notFlagged = dataFrame.selectNotFlagged();
        this.assertInstanceCount(notFlagged, 4);
        assertTrue(notFlagged.isPoolingEnabled());
    }

    private void assertInstanceCount(DataFrame dataFrame, int expectedCount)
    {
        MutableSet<Object> names = this.createIdentitySet().withAll(dataFrame.getStringColumn("name").toList());
        MutableSet<Object> dates = this.createIdentitySet().withAll(dataFrame.getDateColumn("date").toList());

        assertEquals(expectedCount, names.size());
        assertEquals(expectedCount, dates.size());
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
