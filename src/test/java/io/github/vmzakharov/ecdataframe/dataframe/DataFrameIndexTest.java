package io.github.vmzakharov.ecdataframe.dataframe;

import org.eclipse.collections.api.factory.Lists;
import org.eclipse.collections.impl.factory.primitive.IntLists;

import org.junit.jupiter.api.Test;

import static io.github.vmzakharov.ecdataframe.util.FormatWithPlaceholders.messageFromKey;
import static org.junit.jupiter.api.Assertions.*;

public class DataFrameIndexTest
{
    @Test
    public void oneColumnIndex()
    {
        DataFrame dataFrame = new DataFrame("FrameOfData")
                .addStringColumn("Name").addStringColumn("Foo").addLongColumn("Bar").addDoubleColumn("Baz").addIntColumn("Qux")
                .addRow("Alice",   "Pqr",  11L, 20.0, 110)
                .addRow("Carol",   "Abc",  12L, 10.0, 120)
                .addRow("Alice",   "Def",  13L, 25.0, 130)
                .addRow("Carol",   "Xyz",  14L, 40.0, 140)
                .addRow("Carol",   "Xyz",  15L, 40.0, 150)
                .addRow("Abigail", "Def",  16L, 11.0, 160)
                ;

        DfIndex index = new DfIndex(dataFrame, Lists.immutable.of("Name"));

        assertEquals(IntLists.immutable.of(0, 2), index.getRowIndicesAtKey(Lists.immutable.of("Alice")));
        assertEquals(2, index.sizeAt(Lists.immutable.of("Alice")));

        assertEquals(IntLists.immutable.of(1, 3, 4), index.getRowIndicesAtKey(Lists.immutable.of("Carol")));
        assertEquals(3, index.sizeAt(Lists.immutable.of("Carol")));

        assertEquals(IntLists.immutable.of(5), index.getRowIndicesAtKey(Lists.immutable.of("Abigail")));
        assertEquals(1, index.sizeAt(Lists.immutable.of("Abigail")));

        assertEquals(IntLists.immutable.of(), index.getRowIndicesAtKey(Lists.immutable.of("Bob")));

        assertEquals(0, index.sizeAt(Lists.immutable.of("Xavier")));
    }

    @Test
    public void multiColumnIndex()
    {
        DataFrame dataFrame = new DataFrame("FrameOfData")
                .addStringColumn("Name").addStringColumn("Foo").addLongColumn("Bar").addDoubleColumn("Baz")
                .addRow("Alice",   "Def",  1L, 20.0)
                .addRow("Carol",   "Abc",  2L, 10.0)
                .addRow("Abigail", "Xyz",  3L, 11.0)
                .addRow("Alice",   "Def",  4L, 25.0)
                .addRow("Carol",   "Xyz",  5L, 40.0)
                .addRow("Carol",   "Xyz",  6L, 40.0)
                .addRow("Abigail", "Def",  7L, 11.0)
                ;

        DfIndex index = new DfIndex(dataFrame, Lists.immutable.of("Name", "Foo"));

        assertEquals(IntLists.immutable.of(0, 3), index.getRowIndicesAtKey("Alice", "Def"));
        assertEquals(IntLists.immutable.of(0, 3), index.getRowIndicesAtKey(Lists.immutable.of("Alice", "Def")));
        assertEquals(IntLists.immutable.of(), index.getRowIndicesAtKey(Lists.immutable.of("Alice", "Xyz")));
        assertEquals(IntLists.immutable.of(), index.getRowIndicesAtKey(Lists.immutable.of("Alice")));

        assertEquals(IntLists.immutable.of(1), index.getRowIndicesAtKey("Carol", "Abc"));
        assertEquals(IntLists.immutable.of(1), index.getRowIndicesAtKey(Lists.immutable.of("Carol", "Abc")));
        assertEquals(IntLists.immutable.of(4, 5), index.getRowIndicesAtKey(Lists.immutable.of("Carol", "Xyz")));

        assertEquals(IntLists.immutable.of(6), index.getRowIndicesAtKey(Lists.immutable.of("Abigail", "Def")));
    }

    @Test
    public void multiColumnDifferentTypesIndex()
    {
        DataFrame dataFrame = new DataFrame("FrameOfData")
                .addStringColumn("Name").addLongColumn("Quux").addLongColumn("Bar")
                .addRow("Alice",   10L, 1L)
                .addRow("Carol",   20L, 2L)
                .addRow("Abigail", 10L, 3L)
                .addRow("Alice",   10L, 4L)
                .addRow("Carol",   20L, 5L)
                .addRow("Carol",   10L, 6L)
                ;

        DfIndex index = new DfIndex(dataFrame, Lists.immutable.of("Name", "Quux"));

        assertEquals(IntLists.immutable.of(0, 3), index.getRowIndicesAtKey(Lists.immutable.of("Alice", 10L)));
        assertEquals(IntLists.immutable.of(), index.getRowIndicesAtKey(Lists.immutable.of("Alice", 11L)));
        assertEquals(IntLists.immutable.of(), index.getRowIndicesAtKey(Lists.immutable.of("Alice")));

        assertEquals(IntLists.immutable.of(1, 4), index.getRowIndicesAtKey(Lists.immutable.of("Carol", 20L)));
        assertEquals(IntLists.immutable.of(5), index.getRowIndicesAtKey(Lists.immutable.of("Carol", 10L)));

        assertEquals(IntLists.immutable.of(2), index.getRowIndicesAtKey(Lists.immutable.of("Abigail", 10L)));
    }

    @Test
    public void multiColumnDifferentTypesIndexInt()
    {
        DataFrame dataFrame = new DataFrame("FrameOfData")
                .addStringColumn("Name").addIntColumn("Quux").addIntColumn("Bar")
                .addRow("Alice",   10, 1)
                .addRow("Carol",   20, 2)
                .addRow("Abigail", 10, 3)
                .addRow("Alice",   10, 4)
                .addRow("Carol",   20, 5)
                .addRow("Carol",   10, 6)
                ;

        DfIndex index = new DfIndex(dataFrame, Lists.immutable.of("Name", "Quux"));

        assertEquals(IntLists.immutable.of(0, 3), index.getRowIndicesAtKey(Lists.immutable.of("Alice", 10)));
        assertEquals(IntLists.immutable.of(), index.getRowIndicesAtKey(Lists.immutable.of("Alice", 11)));
        assertEquals(IntLists.immutable.of(), index.getRowIndicesAtKey(Lists.immutable.of("Alice")));

        assertEquals(IntLists.immutable.of(1, 4), index.getRowIndicesAtKey(Lists.immutable.of("Carol", 20)));
        assertEquals(IntLists.immutable.of(5), index.getRowIndicesAtKey(Lists.immutable.of("Carol", 10)));

        assertEquals(IntLists.immutable.of(2), index.getRowIndicesAtKey(Lists.immutable.of("Abigail", 10)));
    }

    @Test
    public void dataFrameWrappedIndex()
    {
        DataFrame dataFrame = new DataFrame("FrameOfData")
                .addStringColumn("Name").addStringColumn("Foo").addLongColumn("Bar").addDoubleColumn("Baz")
                .addRow("Alice",   "Pqr",  11L, 20.0)
                .addRow("Carol",   "Abc",  12L, 10.0)
                .addRow("Alice",   "Def",  13L, 25.0)
                .addRow("Carol",   "Xyz",  14L, 40.0)
                .addRow("Carol",   "Xyz",  15L, 40.0)
                .addRow("Abigail", "Def",  16L, 11.0)
                ;

        dataFrame.createIndex("ByName", Lists.immutable.of("Name"));

        assertEquals(IntLists.immutable.of(0, 2), dataFrame.index("ByName").getRowIndicesAtKey(Lists.immutable.of("Alice")));
        assertEquals(2, dataFrame.index("ByName").sizeAt(Lists.immutable.of("Alice")));

        StringBuilder carols = new StringBuilder();
        dataFrame.index("ByName")
             .iterateAt("Carol")
             .forEach(c -> carols.append(c.getString("Name")));

        assertEquals("CarolCarolCarol", carols.toString());

        dataFrame.dropIndex("ByName");

        Exception e = assertThrows(RuntimeException.class, () -> dataFrame.index("ByName").sizeAt("Carol"));

        assertEquals(
            messageFromKey("DF_INDEX_DOES_NOT_EXIST")
                .with("indexName", "ByName")
                .with("dataFrameName", dataFrame.getName())
                .toString(),
            e.getMessage());
    }
}
