package org.modelscript.dataframe;

import java.util.Arrays;

public class DfTuple
implements Comparable<DfTuple>
{
    private final Comparable<Object>[] items;
    private final int rowIndex;

    public DfTuple(Comparable<Object>[] newItems, int newRowIndex)
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
        Comparable<Object>[] these = this.items;
        Comparable<Object>[] others = that.items;

        if (these == others)
        {
            return 0;
        }

        for (int i = 0; i < these.length; i++)
        {
            Comparable<Object> thisItem = these[i];
            Comparable<Object> thatItem = others[i];

            int result = thisItem.compareTo(thatItem);
            if (result != 0)
            {
                return result;
            }
        }

        return 0;
    }
}
