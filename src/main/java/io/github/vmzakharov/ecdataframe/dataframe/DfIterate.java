package io.github.vmzakharov.ecdataframe.dataframe;

import org.eclipse.collections.api.block.function.Function;
import org.eclipse.collections.api.block.procedure.Procedure;
import org.eclipse.collections.api.list.ListIterable;
import org.eclipse.collections.impl.factory.Lists;

import java.util.function.BiConsumer;
import java.util.function.Supplier;

/**
 * An interface defining common iteration patterns for data frames and data frame indices
 */
public interface DfIterate
{
    /**
     * Executes the procedure for each row in this dataframe iterable
     *
     * @param action the procedure to execute for each row
     */
    void forEach(Procedure<DfCursor> action);

    /**
     * Creates a list from this dataframe iterable. The new collection is made up of the results of applying
     * the specified function to each row of the data frame iterable.
     *
     * @param action the function the result of which will be collected in the resulting collection
     * @param <V>    the type of the elements of the resulting collection
     * @return the list made up of the results of applying the specified action to each row of the data frame iterable
     */
    default <V> ListIterable<V> collect(Function<DfCursor, ? extends V> action)
    {
        return this.collect(
                Lists.mutable::empty,
                (container, row) -> container.add(action.valueOf(row))
        );
    }

    /**
     * Performs a reduction operation on each row of the data frame iterable with the results accumulating in the
     * container provided by the supplier parameter.
     * @param supplier supplies the result container
     * @param accumulator a two argument function that takes the result container and the current row of the data frame
     *                    iterable
     * @return the result container
     * @param <R> the type of the result reduction operation (result container)
     */
    default <R> R collect(Supplier<? extends R> supplier, BiConsumer<R, DfCursor> accumulator)
    {
        R result = supplier.get();

        this.forEach(row -> accumulator.accept(result, row));

        return result;
    }
}
