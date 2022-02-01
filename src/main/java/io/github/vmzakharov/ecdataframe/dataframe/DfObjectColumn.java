package io.github.vmzakharov.ecdataframe.dataframe;

import org.eclipse.collections.api.list.ImmutableList;

public interface DfObjectColumn<T>
extends DfColumn
{
    ImmutableList<T> toList();

    T getTypedObject(int rowIndex);

    @Override
    default Object getObject(int rowIndex)
    {
        return this.getTypedObject(rowIndex);
    }
}
