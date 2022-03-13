package io.github.vmzakharov.ecdataframe.dataframe;

import io.github.vmzakharov.ecdataframe.dsl.value.Value;
import org.eclipse.collections.api.list.ImmutableList;

public interface DfObjectColumn<T>
extends DfColumn
{
    ImmutableList<T> toList();

    T getTypedObject(int rowIndex);

    Value objectToValue(T anObject);

    @Override
    default Object getObject(int rowIndex)
    {
        return this.getTypedObject(rowIndex);
    }

    @Override
    default Object aggregate(AggregateFunction aggregateFunction)
    {
        if (this.getSize() == 0)
        {
            return aggregateFunction.defaultObjectIfEmpty();
        }

        try
        {
            return aggregateFunction.applyToObjectColumn(this);
        }
        catch (NullPointerException npe)
        {
            return null;
        }
    }
}
