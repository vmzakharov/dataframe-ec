package io.github.vmzakharov.ecdataframe.dataframe;

import org.eclipse.collections.api.list.ListIterable;
import org.eclipse.collections.api.list.MutableList;
import org.eclipse.collections.api.list.primitive.IntList;
import org.eclipse.collections.api.list.primitive.MutableIntList;
import org.eclipse.collections.api.map.MutableMap;
import org.eclipse.collections.impl.factory.Lists;
import org.eclipse.collections.impl.factory.Maps;
import org.eclipse.collections.impl.factory.primitive.IntLists;

public class DfIndex
{
    static private final MutableIntList EMPTY_LIST = IntLists.mutable.empty().asUnmodifiable();

    private final MutableMap<ListIterable<Object>, MutableIntList> rowIndexByKey = Maps.mutable.of();

    public DfIndex(DataFrame newIndexedDataFrame, ListIterable<String> indexByColumnNames)
    {
        this.buildIndex(newIndexedDataFrame, indexByColumnNames);
    }

    private void buildIndex(DataFrame dataFrame, ListIterable<String> indexByColumnNames)
    {
        ListIterable<DfColumn> indexByColumns = indexByColumnNames.collect(dataFrame::getColumnNamed);

        int rowCount = dataFrame.rowCount();
        for (int rowIndex = 0; rowIndex < rowCount; rowIndex++)
        {
            this.addRowIndex(
                    this.computeKeyFrom(indexByColumns, rowIndex),
                    rowIndex
            );
        }
    }

    public IntList getRowIndicesAtKey(ListIterable<Object> key)
    {
        return this.rowIndexByKey.getIfAbsent(key, () -> EMPTY_LIST);
    }

    private ListIterable<Object> computeKeyFrom(ListIterable<DfColumn> indexByColumns, int rowIndex)
    {
        MutableList<Object> key = Lists.fixedSize.of(new Object[indexByColumns.size()]);

        indexByColumns.forEachWithIndex((col, index) -> key.set(index, col.getObject(rowIndex)));

        return key;
    }

    public void addRowIndex(ListIterable<Object> key, int rowIndex)
    {
        MutableIntList rowIndicesAtKey = this.rowIndexByKey.getIfAbsentPut(key, IntLists.mutable::of);
        rowIndicesAtKey.add(rowIndex);
    }
}
