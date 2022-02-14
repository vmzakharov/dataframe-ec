package io.github.vmzakharov.ecdataframe.dataframe;

import org.eclipse.collections.api.list.ListIterable;
import org.eclipse.collections.api.list.MutableList;
import org.eclipse.collections.api.map.primitive.MutableObjectIntMap;
import org.eclipse.collections.impl.factory.Lists;
import org.eclipse.collections.impl.factory.primitive.ObjectIntMaps;

/**
 * Maintains an index on a data frame based on one or more column values. If a row with a specified index value does
 * not exist, will add the row with the respective values to the data frame. This can be useful for example for
 * aggregation to maintain a dataframe with accumulator rows.
 */
public class DfIndexKeeper
{
    private final MutableObjectIntMap<ListIterable<Object>> rowIndexByKey = ObjectIntMaps.mutable.of();
    private final ListIterable<DfColumn> columnsToIndexBy;
    private final DataFrame indexedDataFrame;

    public DfIndexKeeper(DataFrame newIndexedDataFrame, ListIterable<String> indexByColumnNames)
    {
        this.indexedDataFrame = newIndexedDataFrame;
        this.columnsToIndexBy = indexByColumnNames.collect(this.indexedDataFrame::getColumnNamed);
    }

    private int getRowIndexAtKey(ListIterable<Object> key)
    {
        return this.rowIndexByKey.getIfAbsent(key, -1);
    }

    /**
     * Returns the value in the index that matches the index key. If no matching key exists in the index, a new entry
     * for this key is added to the indexed data frame and its index is returned.
     * @param key the to look up the matching row in the indexed dataframe by
     * @return row id in the <b>the indexed data frame</b>  corresponding to the computed key
     */
    public int getRowIndexAtKeyIfAbsentAdd(ListIterable<Object> key)
    {
        int rowIndex = this.getRowIndexAtKey(key);
        if (rowIndex == -1)
        {
            this.indexedDataFrame.addRow();

            // need to be effectively final so introducing another variable rather than reusing rowIndex
            int lastRowIndex = this.indexedDataFrame.rowCount() - 1;
            this.addIndex(key, lastRowIndex);
            // todo - use addObject/add Empty instead?
            this.columnsToIndexBy.forEachWithIndex((col, i) -> col.setObject(lastRowIndex, key.get(i)));

            rowIndex = lastRowIndex;
        }
        return rowIndex;
    }

    /**
     * Checks if the given key exists in this index
     * @param key the key to look up in the index
     * @return <code>true</code> if the key exists in the index, <code>false</code> otherwise
     */
    public boolean contains(ListIterable<Object> key)
    {
        return this.getRowIndexAtKey(key) != -1;
    }

    /**
     * Checks if the given key <b>does not</b> exist in this index
     * @param key the key to look up in the index
     * @return <code>true</code> if the key does not exist in the index, <code>false</code> otherwise
     */
    public boolean doesNotContain(ListIterable<Object> key)
    {
        return this.getRowIndexAtKey(key) == -1;
    }

    public ListIterable<Object> computeKeyFrom(DataFrame aDataFrame, int rowIndex)
    {
        MutableList<Object> key = Lists.fixedSize.of(new Object[this.columnsToIndexBy.size()]);

        this.columnsToIndexBy.forEachWithIndex(
                (col, index) -> key.set(index, aDataFrame.getColumnNamed(col.getName()).getObject(rowIndex)));
        return key;
    }

    public void addIndex(ListIterable<Object> key, int rowIndex)
    {
        this.rowIndexByKey.put(key, rowIndex);
    }
}
