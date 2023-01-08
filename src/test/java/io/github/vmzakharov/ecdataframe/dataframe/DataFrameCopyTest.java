package io.github.vmzakharov.ecdataframe.dataframe;

import io.github.vmzakharov.ecdataframe.dsl.value.Value;
import org.eclipse.collections.api.factory.Lists;
import org.eclipse.collections.api.list.ImmutableList;
import org.junit.Assert;
import org.junit.Test;

public class DataFrameCopyTest
{
    @Test
    public void copySchema()
    {
        DataFrame df = new DataFrame("df1");
        df.addStringColumn("Name").addLongColumn("Count").addDoubleColumn("Value");
        df
                .addRow("Alice", 5, 23.45)
                .addRow("Bob",  10, 12.34)
                .addRow("Carl", 11, 56.78)
                .addRow("Deb",   0,  7.89);

        df.addDoubleColumn("Twice", "Value * 2");

        DataFrame clonedSchema = df.cloneStructure("Cloned");

        Assert.assertEquals(0, clonedSchema.rowCount());
        Assert.assertEquals(df.columnCount(), clonedSchema.columnCount());
        for (int i = 0; i < df.columnCount(); i++)
        {
            DfColumn sourceColumn = df.getColumnAt(i);
            DfColumn copyColumn = clonedSchema.getColumnAt(i);

            Assert.assertEquals(sourceColumn.getName(), copyColumn.getName());
            if (sourceColumn.isComputed())
            {
                Assert.assertTrue(copyColumn.isComputed());
                Assert.assertEquals(
                        ((DfColumnComputed) sourceColumn).getExpressionAsString(),
                        ((DfColumnComputed) copyColumn).getExpressionAsString());
            }
            Assert.assertSame(clonedSchema, copyColumn.getDataFrame());
        }
    }

    @Test
    public void copySchemaConvertComputedToStored()
    {
        DataFrame df = new DataFrame("df1");
        df.addStringColumn("Name").addLongColumn("Count").addDoubleColumn("Value");
        df
                .addRow("Alice", 5, 23.45)
                .addRow("Bob",  10, 12.34)
                .addRow("Carl", 11, 56.78)
                .addRow("Deb",   0,  7.89);

        df.addDoubleColumn("Twice", "Value * 2");

        DataFrame clonedSchema = df.cloneStructureAsStored("Cloned");

        Assert.assertEquals(0, clonedSchema.rowCount());
        Assert.assertEquals(df.columnCount(), clonedSchema.columnCount());
        for (int i = 0; i < df.columnCount(); i++)
        {
            DfColumn sourceColumn = df.getColumnAt(i);
            DfColumn copyColumn = clonedSchema.getColumnAt(i);

            Assert.assertEquals(sourceColumn.getName(), copyColumn.getName());
            Assert.assertEquals(sourceColumn.getType(), copyColumn.getType());
            Assert.assertTrue(copyColumn.isStored());
            Assert.assertSame(clonedSchema, copyColumn.getDataFrame());
        }
    }

    @Test
    public void copyWholeDataFrame()
    {
        DataFrame df = new DataFrame("df1");
        df.addStringColumn("Name").addLongColumn("Count").addDoubleColumn("Value");
        df
                .addRow("Alice", 5, 23.45)
                .addRow("Bob",  10, 12.34)
                .addRow("Carl", 11, 56.78)
                .addRow("Deb",   0,  7.89);

        df.addDoubleColumn("Twice", "Value * 2");

        DataFrame copiedSchema = df.copy("MyNewCopy");

        Assert.assertEquals(0, copiedSchema.rowCount());
        Assert.assertEquals(df.columnCount(), copiedSchema.columnCount());
        for (int i = 0; i < df.columnCount(); i++)
        {
            DfColumn sourceColumn = df.getColumnAt(i);
            DfColumn copyColumn = copiedSchema.getColumnAt(i);

            Assert.assertEquals(sourceColumn.getName(), copyColumn.getName());
            Assert.assertEquals(sourceColumn.getType(), copyColumn.getType());
            Assert.assertTrue(copyColumn.isStored());
            Assert.assertSame(copiedSchema, copyColumn.getDataFrame());

            for (int j = 0; j < df.rowCount(); j++)
            {
                Value cellValue = df.getColumnAt(i).getValue(j);
                Value copiedCellValue = copiedSchema.getColumnAt(i).getValue(j);

                Assert.assertEquals(0, cellValue.compareTo(copiedCellValue));
            }
        }
    }

    @Test
    public void copySomeColumnsToANewDataFrame()
    {
        DataFrame df = new DataFrame("df1");
        df.addStringColumn("Name").addLongColumn("Count").addDoubleColumn("Value");
        df
                .addRow("Alice", 5, 23.45)
                .addRow("Bob",  10, 12.34)
                .addRow("Carl", 11, 56.78)
                .addRow("Deb",   0,  7.89);

        df.addDoubleColumn("Twice", "Value * 2");

        ImmutableList<String> columnNamesToCopy = Lists.immutable.of("Name", "Twice");
        DataFrame copiedSchema = df.copy("MyNewCopy", columnNamesToCopy);

        Assert.assertEquals(0, copiedSchema.rowCount());
        Assert.assertEquals(columnNamesToCopy.size(), copiedSchema.columnCount());
        for (int i = 0; i < columnNamesToCopy.size(); i++)
        {
            DfColumn copyColumn = copiedSchema.getColumnAt(i);
            DfColumn sourceColumn = df.getColumnNamed(copyColumn.getName());

            Assert.assertEquals(sourceColumn.getName(), copyColumn.getName());
            Assert.assertEquals(sourceColumn.getType(), copyColumn.getType());
            Assert.assertTrue(copyColumn.isStored());

            for (int j = 0; j < df.rowCount(); j++)
            {
                Value cellValue = df.getColumnNamed(copyColumn.getName()).getValue(j);
                Value copiedCellValue = copiedSchema.getColumnAt(i).getValue(j);

                Assert.assertEquals(0, cellValue.compareTo(copiedCellValue));
            }
        }
    }
}
