package io.github.vmzakharov.ecdataframe.dataframe;

import org.eclipse.collections.api.list.ListIterable;
import org.eclipse.collections.impl.factory.Lists;

/**
 * A utility class, instances of which are used to define and execute a lookup join. A lookup join is used to enrich a
 * data frame (joined-to) by adding one or more columns based on value lookup in another data frame (joined-from)
 */
final public class DfJoin
{
    private DataFrame joinFrom;
    private DataFrame joinTo;
    private ListIterable<String> selectFromJoined;
    private ListIterable<String> columnNameAliases;
    private ListIterable<Object> valuesIfAbsent;
    private ListIterable<String> joinToColumnNames;
    private ListIterable<String> columnsToLookup;

    private DfJoin()
    {
        // uses the "to" factory methods to construct a new instance
    }

    /**
     * A factory method to create a descriptor of a join to the given data frame
     * @param newJoinTo the data frame to join to
     * @return a new join descriptor
     */
    static public DfJoin to(DataFrame newJoinTo)
    {
        DfJoin join = new DfJoin();
        join.joinTo = newJoinTo;
        return join;
    }

    /**
     * A factory method to create a descriptor of a join from the given data frame
     * @param newJoinFrom the data frame to join from
     * @return a new join descriptor
     */
    static public DfJoin from(DataFrame newJoinFrom)
    {
        DfJoin join = new DfJoin();
        join.joinFrom = newJoinFrom;
        return join;
    }

    /**
     * Specifies the data frame to join to
     * @param newJoinTo the data frame to join to
     * @return this join descriptor
     */
    public DfJoin joinTo(DataFrame newJoinTo)
    {
        this.joinTo = newJoinTo;
        return this;
    }

    /**
     * Specifies the join key - a column in the data frame to join from to be matched to a column in the data frame to
     * join to
     * @param columnToLookup the column in the source data frame to be used as the join key
     * @param joinToColumnName the column in the target data frame to be used as the join key
     * @return this join descriptor
     */
    public DfJoin match(String columnToLookup, String joinToColumnName)
    {
        return this.match(Lists.fixedSize.of(columnToLookup), Lists.fixedSize.of(joinToColumnName));
    }

    /**
     * Specifies the join keys - a list of columns in the data frame to join from to be matched to a corresponding list
     * of columns in the data frame to join to
     * @param newColumnsToLookup the columns in the data frame to join to (enriched) to  be used as the join key
     * @param newJoinToColumnNames the column in the data frame to join from be used as the join key
     * @return this join descriptor
     */
    public DfJoin match(ListIterable<String> newColumnsToLookup, ListIterable<String> newJoinToColumnNames)
    {
        this.columnsToLookup = newColumnsToLookup;
        this.joinToColumnNames = newJoinToColumnNames;
        return this;
    }

    /**
     * Specifies the name of the column to add to the join-to data frame from the join-from data frame
     * @param newSelectFromJoined the column to add to the source data frame from the join target data frame
     * @return this join descriptor
     */
    public DfJoin select(String newSelectFromJoined)
    {
        return this.select(newSelectFromJoined, newSelectFromJoined);
    }

    /**
     * Specifies the column to add to the join-to data frame from the join-from data frame, and the name the added
     * column will have in the enriched join-to data frame.
     * @param newSelectFromJoined the column to add to the join-to data frame from the join-from data frame
     * @param newAlias the name of the added column in the enriched join-to data frame
     * @return this join descriptor
     */
    public DfJoin select(String newSelectFromJoined, String newAlias)
    {
        return this.select(Lists.fixedSize.of(newSelectFromJoined), Lists.fixedSize.of(newAlias));
    }

    /**
     * Specifies the name of the column to add to the join-to data frame from the join-from data frame
     * @param newSelectFromJoined the names of the columns to be added to the data frame to join to (data frame being
     *                        enriched)
     * @return this join descriptor
     */
    public DfJoin select(ListIterable<String> newSelectFromJoined)
    {
        return this.select(newSelectFromJoined, newSelectFromJoined);
    }

    /**
     * Specifies the name of the column to add to the join-to data frame from the join-from data frame and the new column
     * names to be used in the enriched data frame
     * @param newSelectFromJoined the names of the columns to be selected from the data frame-from and added to the data
     *                           frame to join to (that is to the data frame being enriched)
     * @param newColumnNameAliases the aliases of the columns being added
     * @return this join descriptor
     */
    public DfJoin select(ListIterable<String> newSelectFromJoined, ListIterable<String> newColumnNameAliases)
    {
        this.selectFromJoined = newSelectFromJoined;
        this.columnNameAliases = newColumnNameAliases;
        return this;
    }

    /**
     * The default value to be used for the joined column in the rows where the data frame to merge to does not have the
     * matching join key value
     * @param valueIfAbsent default value to use when matching keys cannot be found
     * @return this join descriptor
     */
    public DfJoin ifAbsent(Object valueIfAbsent)
    {
        return this.ifAbsent(Lists.fixedSize.of(valueIfAbsent));
    }

    /**
     * The default value to be used for the joined columns in the rows where the data frame to merge to does not have
     * the matching join key values
     * @param newValuesIfAbsent default values to use when matching keys cannot be found
     * @return this join descriptor
     */
    public DfJoin ifAbsent(ListIterable<Object> newValuesIfAbsent)
    {
        this.valuesIfAbsent = newValuesIfAbsent;
        return this;
    }

    /**
     * Returns the data frame to join to - that is the data frame the columns of which will be combined with the
     * dataframe being enriched (the join-from data frame)
     * @return the data frame to join to
     */
    public DataFrame joinTo()
    {
        return this.joinTo;
    }

    /**
     * @return the names of the join key columns in the data frame to join to
     */
    public ListIterable<String> joinToColumnNames()
    {
        return this.joinToColumnNames;
    }

    /**
     * @return  default values to use for join columns when matching key values cannot be found
     */
    public ListIterable<Object> valuesIfAbsent()
    {
        if (this.valuesIfAbsent == null)
        {
            this.valuesIfAbsent = Lists.mutable.of(new Object[this.selectFromJoined.size()]);
        }
        return this.valuesIfAbsent;
    }

    /**
     * @return The names of the columns in the data frame-from to be joined to the data frame being enriched
     */
    public ListIterable<String> selectFromJoined()
    {
        return this.selectFromJoined;
    }

    /**
     * @return the new names of the columns added to the enriched data frame
     */
    public ListIterable<String> columnNameAliases()
    {
        return this.columnNameAliases;
    }

    /**
     * @return the columns in the data frame to join to (enriched) to  be used as the join key
     */
    public ListIterable<String> columnsToLookup()
    {
        return this.columnsToLookup;
    }

    /**
     * Execute the lookup join as defined by this descriptor.
     * @return the enriched join-to data frame
     */
    public DataFrame resolveLookup()
    {
        return this.joinFrom.lookup(this);
    }
}
