package io.github.vmzakharov.ecdataframe.dataframe;

import org.eclipse.collections.api.block.procedure.Procedure;
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

    private DataFrame dataFrame;

    public DfIndex(DataFrame newIndexedDataFrame, ListIterable<String> indexByColumnNames)
    {
        this.buildIndex(newIndexedDataFrame, indexByColumnNames);
    }

    private void buildIndex(DataFrame newDataFrame, ListIterable<String> indexByColumnNames)
    {
        this.dataFrame = newDataFrame;

        ListIterable<DfColumn> indexByColumns = indexByColumnNames.collect(newDataFrame::getColumnNamed);

        int rowCount = newDataFrame.rowCount();
        for (int rowIndex = 0; rowIndex < rowCount; rowIndex++)
        {
            this.addRowIndex(
                this.computeKeyFrom(indexByColumns, rowIndex),
                rowIndex
            );
        }
    }

    public IntList getRowIndicesAtKey(Object... keys)
    {
        return this.getRowIndicesAtKey(Lists.immutable.of(keys));
    }

    public IntList getRowIndicesAtKey(ListIterable<Object> key)
    {
        return this.rowIndexByKey.getIfAbsent(key, () -> EMPTY_LIST);
    }

    public int sizeAt(Object... keys)
    {
        return this.sizeAt(Lists.immutable.of(keys));
    }

    /**
     * returns the number of data frame rows corresponding to this index value
     * @param key the index value to look up
     * @return the number of data frame rows corresponding to this index value
     */
    public int sizeAt(ListIterable<Object> key)
    {
        return this.rowIndexByKey.getIfAbsent(key, () -> EMPTY_LIST).size();
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

    public DfIndexIterator iterateAt(Object... keys)
    {
        return this.iterateAt(Lists.immutable.with(keys));
    }

    public DfIndexIterator iterateAt(ListIterable<Object> key)
    {
        return new DfIndexIterator(this.getRowIndicesAtKey(key));
    }

    public class DfIndexIterator
    implements DfIterate
    {
        private final IntList indexes;

        public DfIndexIterator(IntList newIndexes)
        {
            this.indexes = newIndexes;
        }

        @Override
        public void forEach(Procedure<DfCursor> action)
        {
            DfCursor cursor = new DfCursor(DfIndex.this.dataFrame);

            this.indexes.forEach(
                i -> action.value(cursor.rowIndex(i))
            );
        }
    }
}
