package org.modelscript.dataframe;

import org.eclipse.collections.impl.utility.ArrayIterate;
import org.modelscript.expr.value.Value;

import java.util.Arrays;

public class DfTuple
implements Comparable<DfTuple>
{
    private final Comparable[] items;
    private final int rowIndex;

    public DfTuple(int newRowIndex, Comparable... newItems)
    {
        this.items = newItems;
        this.rowIndex = newRowIndex;
    }

    public int getRowIndex()
    {
        return this.rowIndex;
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) { return true; }
        if (o == null || getClass() != o.getClass()) { return false; }
        DfTuple dfTuple = (DfTuple) o;
        return Arrays.equals(items, dfTuple.items);
    }

    @Override
    public int hashCode()
    {
        return Arrays.hashCode(items);
    }

    @Override
    public int compareTo(DfTuple that)
    {
        Comparable[] these = this.items;
        Comparable[] others = that.items;

        if (these == others)
        {
            return 0;
        }

        for (int i = 0; i < these.length; i++)
        {
            Comparable thisItem = these[i];
            Comparable thatItem = others[i];

            int result = thisItem.compareTo(thatItem);
            if (result != 0)
            {
                return result;
            }
        }

        return 0;
    }

    @Override
    public String toString()
    {
        return this.rowIndex + ": " + ArrayIterate.makeString(this.items);
    }
}
