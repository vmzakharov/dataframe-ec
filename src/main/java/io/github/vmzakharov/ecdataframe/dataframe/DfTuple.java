package io.github.vmzakharov.ecdataframe.dataframe;

import org.eclipse.collections.impl.utility.ArrayIterate;

import java.util.Arrays;

public class DfTuple
implements Comparable<DfTuple>
{
    private final Object[] items;

    public DfTuple(Object... newItems)
    {
        this.items = newItems;
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) { return true; }
        if (o == null || this.getClass() != o.getClass()) { return false; }
        DfTuple dfTuple = (DfTuple) o;
        return Arrays.equals(this.items, dfTuple.items);
    }

    @Override
    public int hashCode()
    {
        return Arrays.hashCode(this.items);
    }

    @Override
    public int compareTo(DfTuple that)
    {
        Object[] these = this.items;
        Object[] others = that.items;

        if (these == others)
        {
            return 0;
        }

        for (int i = 0; i < these.length; i++)
        {
            Object thisItem = these[i];
            Object thatItem = others[i];

            if (thisItem == null)
            {
                if (thatItem != null)
                {
                    return -1;
                }
            }
            else if (thatItem == null)
            {
                return 1;
            }
            else
            {
                int result = ((Comparable) thisItem).compareTo(thatItem);
                if (result != 0)
                {
                    return result;
                }
            }
        }

        return 0;
    }

    @Override
    public String toString()
    {
        return ArrayIterate.makeString(this.items);
    }
}
