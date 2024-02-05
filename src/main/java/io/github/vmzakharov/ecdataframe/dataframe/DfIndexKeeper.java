package io.github.vmzakharov.ecdataframe.dataframe;

import org.eclipse.collections.api.block.procedure.primitive.IntProcedure;
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
    private final ListIterable<DfColumn> sourceColumns;

    public DfIndexKeeper(
            DataFrame newIndexedDataFrame,
            ListIterable<String> indexByColumnNames,
            DataFrame newSourceDataFrame)
    {
        this.indexedDataFrame = newIndexedDataFrame;
        this.columnsToIndexBy = indexByColumnNames.collect(this.indexedDataFrame::getColumnNamed);
        this.sourceColumns = indexByColumnNames.collect(newSourceDataFrame::getColumnNamed);
    }

    private int getRowIndexAtKey(ListIterable<Object> key)
    {
        return this.rowIndexByKey.getIfAbsent(key, -1);
    }

    /**
     * Returns the value in the index that matches the index key. If no matching key exists in the index, a new entry
     * for this key is added to the indexed data frame and its index is returned.
     * @param key the key to look up the matching row in the indexed dataframe by
     * @return row id in the <b>the indexed data frame</b>  corresponding to the computed key
     */
    public int getRowIndexAtKeyIfAbsentAdd(ListIterable<Object> key)
    {
        return this.getRowIndexAtKeyIfAbsentAddAndEvaluate(key, i -> { });
    }

    /**
     * Returns the value in the index that matches the index key. If no matching key exists in the index, a new entry
     * for this key is added to the indexed data frame and the procedure passed into this method is evaluated with the
     * newly added row index as its parameter.
     * @param key the key to look up the matching row in the indexed dataframe by
     * @param evaluateIfAbsent the procedure that will be executed if no index at this key is found and the new entry
     *                         needs to be created. This can be used to initialize the just added row in the indexed
     *                         data frame.
     * @return row id in the <b>the indexed data frame</b>  corresponding to the computed key
     */
    public int getRowIndexAtKeyIfAbsentAddAndEvaluate(ListIterable<Object> key, IntProcedure evaluateIfAbsent)
    {
        int rowIndex = this.getRowIndexAtKey(key);

        if (rowIndex == -1)
        {
            this.indexedDataFrame.addRow();

            // need to be effectively final so introducing another variable rather than reusing rowIndex
            int lastRowIndex = this.indexedDataFrame.rowCount() - 1;
            this.addIndex(key, lastRowIndex);

            this.columnsToIndexBy.forEachWithIndex((col, i) -> col.setObject(lastRowIndex, key.get(i)));

            rowIndex = lastRowIndex;

            evaluateIfAbsent.value(rowIndex);
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

    public ListIterable<Object> computeKeyFrom(int rowIndex)
    {
        // TODO: avoid recreating the list - perhaps have a separate lookup key (reusable) vs stored key
        MutableList<Object> key = Lists.fixedSize.of(new Object[this.columnsToIndexBy.size()]);

        this.sourceColumns.forEachWithIndex(
            (col, index) -> key.set(index, col.getObject(rowIndex))
        );

        return key;
    }

    public void addIndex(ListIterable<Object> key, int rowIndex)
    {
        this.rowIndexByKey.put(key, rowIndex);
    }
}
