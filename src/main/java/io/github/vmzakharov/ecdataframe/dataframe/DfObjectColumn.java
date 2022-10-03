package io.github.vmzakharov.ecdataframe.dataframe;

import io.github.vmzakharov.ecdataframe.dsl.value.Value;
import org.eclipse.collections.api.block.function.Function2;
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
        if (aggregateFunction.supportsSourceType(this.getType()))
        {
            if (this.getSize() == 0)
            {
                return aggregateFunction.defaultObjectIfEmpty();
            }

            return aggregateFunction.applyToObjectColumn(this);
        }
        else
        {
            aggregateFunction.throwNotApplicable("values of type " + this.getType());
        }

        return null;
    }

    default <IV> IV injectIntoBreakOnNulls(
            IV injectedValue,
            Function2<IV, T, IV> function
    )
    {
        IV result = injectedValue;

        for (int i = 0; i < this.getSize(); i++)
        {
            T current =  this.getTypedObject(i);
            if (current == null)
            {
                return null;
            }

            result = function.apply(result, current);

            if (result == null)
            {
                return null;
            }
        }

        return result;
    }
}
