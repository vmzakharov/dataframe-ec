package io.github.vmzakharov.ecdataframe.dataframe;

import org.eclipse.collections.api.list.ListIterable;
import org.eclipse.collections.impl.utility.ArrayIterate;

import java.util.Arrays;

public class DfTuple
implements Comparable<DfTuple>
{
    private final Object[] items;
    private final int order;

    public DfTuple(Object... newItems)
    {
        this(0, newItems);
    }

    public DfTuple(int newOrder, Object... newItems)
    {
        this.order = newOrder;
        this.items = newItems;
    }

    public static int compareMindingNulls(Object thisItem, Object thatItem)
    {
        if (thisItem == null)
        {
            return thatItem == null ? 0 : -1;
        }
        else if (thatItem == null)
        {
            return 1;
        }

        return ((Comparable) thisItem).compareTo(thatItem);
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o)
        {
            return true;
        }

        if (o == null || this.getClass() != o.getClass())
        {
            return false;
        }

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
            int result = compareMindingNulls(these[i], others[i]);
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
        return ArrayIterate.makeString(this.items);
    }

    public int order()
    {
        return this.order;
    }

    public int compareTo(DfTuple that, ListIterable<DfColumnSortOrder> sortOrders)
    {
        Object[] these = this.items;
        Object[] others = that.items;

        if (these == others)
        {
            return 0;
        }

        for (int i = 0; i < these.length; i++)
        {
            int result = compareMindingNulls(these[i], others[i]);
            if (result != 0)
            {
                return sortOrders.get(i).order(result);
            }
        }

        return 0;
    }
}
