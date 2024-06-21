package io.github.vmzakharov.ecdataframe.dataframe;

import org.eclipse.collections.api.factory.Lists;
import org.eclipse.collections.api.list.ImmutableList;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

public class DataFrameCopyTest
{
    @Test
    public void copySchema()
    {
        DataFrame df = new DataFrame("df1");
        df
                .addStringColumn("Name")
                .addLongColumn("Count")
                .addIntColumn("IntCount")
                .addDoubleColumn("Value");
        df
                .addRow("Alice", 5, 5, 23.45)
                .addRow("Bob",  10, 10, 12.34)
                .addRow("Carl", 11, 11, 56.78)
                .addRow("Deb",   0, 0, 7.89);

        df.addDoubleColumn("Twice", "Value * 2");

        DataFrame clonedSchema = df.cloneStructure("Cloned");

        assertEquals(0, clonedSchema.rowCount());
        assertEquals(df.columnCount(), clonedSchema.columnCount());
        for (int i = 0; i < df.columnCount(); i++)
        {
            DfColumn sourceColumn = df.getColumnAt(i);
            DfColumn copyColumn = clonedSchema.getColumnAt(i);

            assertEquals(sourceColumn.getName(), copyColumn.getName());
            if (sourceColumn.isComputed())
            {
                assertTrue(copyColumn.isComputed());
                assertEquals(
                        ((DfColumnComputed) sourceColumn).getExpressionAsString(),
                        ((DfColumnComputed) copyColumn).getExpressionAsString());
            }
            assertSame(clonedSchema, copyColumn.getDataFrame());
        }
    }

    @Test
    public void copySchemaConvertComputedToStored()
    {
        DataFrame df = new DataFrame("df1");
        df
                .addStringColumn("Name")
                .addLongColumn("Count")
                .addIntColumn("IntCount")
                .addDoubleColumn("Value");
        df
                .addRow("Alice", 5, 5, 23.45)
                .addRow("Bob",  10, 10, 12.34)
                .addRow("Carl", 11, 11, 56.78)
                .addRow("Deb",   0, 0, 7.89);

        df.addDoubleColumn("Twice", "Value * 2");

        DataFrame clonedSchema = df.cloneStructureAsStored("Cloned");

        assertEquals(0, clonedSchema.rowCount());
        assertEquals(df.columnCount(), clonedSchema.columnCount());
        for (int i = 0; i < df.columnCount(); i++)
        {
            DfColumn sourceColumn = df.getColumnAt(i);
            DfColumn copyColumn = clonedSchema.getColumnAt(i);

            assertEquals(sourceColumn.getName(), copyColumn.getName());
            assertEquals(sourceColumn.getType(), copyColumn.getType());
            assertTrue(copyColumn.isStored());
            assertSame(clonedSchema, copyColumn.getDataFrame());
        }
    }

    @Test
    public void copyWholeDataFrame()
    {
        DataFrame df = new DataFrame("df1");
        df
                .addStringColumn("Name")
                .addLongColumn("Count")
                .addIntColumn("IntCount")
                .addDoubleColumn("Value");
        df
                .addRow("Alice", 5, 5, 23.45)
                .addRow("Bob",  10, 10, 12.34)
                .addRow("Carl", 11, 11, 56.78)
                .addRow("Deb",   0, 0, 7.89);

        df.addDoubleColumn("Twice", "Value * 2");

        DataFrame copiedDataFrame = df.copy("MyNewCopy");

        DataFrameUtil.assertEquals(df, copiedDataFrame);

    }

    @Test
    public void copySomeColumnsToANewDataFrame()
    {
        DataFrame originalDataFrame = new DataFrame("df1");
        originalDataFrame
                .addStringColumn("Name")
                .addLongColumn("Count")
                .addIntColumn("IntCount")
                .addDoubleColumn("Value");

        originalDataFrame
                .addRow("Alice", 5, 5, 23.45)
                .addRow("Bob",  10, 10, 12.34)
                .addRow("Carl", 11, 11, 56.78)
                .addRow("Deb",   0, 0, 7.89);

        originalDataFrame.addDoubleColumn("Twice", "Value * 2");

        DataFrame expectedDataFrame = new DataFrame("df2");
        expectedDataFrame
                .addStringColumn("Name")
                .addDoubleColumn("Twice");

        expectedDataFrame
                .addRow("Alice",  46.9)
                .addRow("Bob",    24.68)
                .addRow("Carl",  113.56)
                .addRow("Deb",    15.78);

        ImmutableList<String> columnNamesToCopy = Lists.immutable.of("Name", "Twice");
        DataFrame copiedDataFrame = originalDataFrame.copy("MyNewCopy", columnNamesToCopy);

        DataFrameUtil.assertEquals(expectedDataFrame, copiedDataFrame);
    }
}
