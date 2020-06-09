package org.modelscript.dataframe;

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

        DataFrame clonedSchema = df.cloneStructure();

        Assert.assertEquals(0, clonedSchema.rowCount());
        Assert.assertEquals(df.columnCount(), clonedSchema.columnCount());
        for (int i= 0; i < df.columnCount(); i++)
        {
            DfColumn sourceColumn = df.getColumnAt(i);
            DfColumn copyColumn = clonedSchema.getColumnAt(i);

            Assert.assertEquals(sourceColumn.getName(), copyColumn.getName());
            if (sourceColumn.isComputed())
            {
                Assert.assertEquals(
                        ((DfColumnComputed) sourceColumn).getExpressionAsString(),
                        ((DfColumnComputed) copyColumn).getExpressionAsString());
            }
            Assert.assertSame(clonedSchema, copyColumn.getDataFrame());
        }
    }
}
