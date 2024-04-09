package io.github.vmzakharov.ecdataframe.dataframe;

import org.eclipse.collections.api.list.ListIterable;
import org.eclipse.collections.impl.utility.ArrayIterate;

import java.util.Arrays;

public class DfTuple
implements Comparable<DfTuple>
{
    private final Object[] items;
    private final int order;

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
        if (o instanceof DfTuple dfTuple)
        {
            return Arrays.equals(this.items, dfTuple.items);
        }

        return false;
    }

    @Override
    public int hashCode()
    {
        return Arrays.hashCode(this.items);
    }

    @Override
    public String toString()
    {
        return this.order + ":" + ArrayIterate.makeString(this.items);
    }

    public int order()
    {
        return this.order;
    }

    @Override
    public int compareTo(DfTuple other)
    {
        return this.compareTo(other, null);
    }

    public int compareTo(DfTuple other, ListIterable<DfColumnSortOrder> sortOrders)
    {
        Object[] theseItems = this.items;

        for (int i = 0; i < theseItems.length; i++)
        {
            int result = compareMindingNulls(theseItems[i], other.items[i]);

            if (result != 0)
            {
                return sortOrders == null ? result : sortOrders.get(i).order(result);
            }
        }

        return 0;
    }
}
