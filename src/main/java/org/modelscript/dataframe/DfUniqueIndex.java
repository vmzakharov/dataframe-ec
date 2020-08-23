package org.modelscript.dataframe;

import org.eclipse.collections.api.list.ListIterable;
import org.eclipse.collections.api.list.MutableList;
import org.eclipse.collections.api.map.primitive.MutableObjectIntMap;
import org.eclipse.collections.impl.factory.Lists;
import org.eclipse.collections.impl.factory.primitive.ObjectIntMaps;

public class DfUniqueIndex
{
    private final MutableObjectIntMap<ListIterable<Object>> rowIndexByKey = ObjectIntMaps.mutable.of();
    private final ListIterable<DfColumn> indexByColumns;
    private final DataFrame indexedDataFrame;

    public DfUniqueIndex(DataFrame newIndexedDataFrame, ListIterable<String> indexByColumnNames)
    {
        this.indexedDataFrame = newIndexedDataFrame;
        this.indexByColumns = indexByColumnNames.collect(this.indexedDataFrame::getColumnNamed);
    }

    private int getRowIndexAtKey(ListIterable<Object> key)
    {
        return this.rowIndexByKey.getIfAbsent(key, -1);
    }

    /**
     * Returns the value in the index that matches the index key. If no matching key exists in the index, a new entry
     * for this key is added to the indexed data frame and its index is returned.
     * @param key the row in the <code>aDataFrame</code> to compute the key on
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
            this.indexByColumns.forEachWithIndex((col, i) -> col.setObject(lastRowIndex, key.get(i)));

            rowIndex = lastRowIndex;
        }
        return rowIndex;
    }

    public ListIterable<Object> computeKeyFrom(DataFrame aDataFrame, int rowIndex)
    {
        MutableList<Object> key = Lists.fixedSize.of(new Object[this.indexByColumns.size()]);

        this.indexByColumns.forEachWithIndex(
                (col, index) -> key.set(index, aDataFrame.getColumnNamed(col.getName()).getObject(rowIndex)));
        return key;
    }

    public void addIndex(ListIterable<Object> key, int rowIndex)
    {
        this.rowIndexByKey.put(key, rowIndex);
    }
}
