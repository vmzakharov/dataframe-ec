package io.github.vmzakharov.ecdataframe.dataframe;

import org.eclipse.collections.api.list.ListIterable;
import org.eclipse.collections.impl.factory.Lists;

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

    static public DfJoin to(DataFrame newJoinTo)
    {
        DfJoin join = new DfJoin();
        join.joinTo = newJoinTo;
        return join;
    }

    static public DfJoin from(DataFrame newJoinFrom)
    {
        DfJoin join = new DfJoin();
        join.joinFrom = newJoinFrom;
        return join;
    }

    public DfJoin joinTo(DataFrame newJoinTo)
    {
        this.joinTo = newJoinTo;
        return this;
    }

    public DfJoin match(String columnToLookup, String joinToColumnName)
    {
        return this.match(Lists.fixedSize.of(columnToLookup), Lists.fixedSize.of(joinToColumnName));
    }

    public DfJoin match(ListIterable<String> newColumnsToLookup, ListIterable<String> newJoinToColumnNames)
    {
        this.columnsToLookup = newColumnsToLookup;
        this.joinToColumnNames = newJoinToColumnNames;
        return this;
    }

    public DfJoin select(String newSelectFromJoined)
    {
        return this.select(newSelectFromJoined, newSelectFromJoined);
    }

    public DfJoin select(String newSelectFromJoined, String newAlias)
    {
        return this.select(Lists.fixedSize.of(newSelectFromJoined), Lists.fixedSize.of(newAlias));
    }

    public DfJoin select(ListIterable<String> newSelectFromJoined)
    {
        return this.select(newSelectFromJoined, newSelectFromJoined);
    }

    public DfJoin select(ListIterable<String> newSelectFromJoined, ListIterable<String> newColumnNameAliases)
    {
        this.selectFromJoined = newSelectFromJoined;
        this.columnNameAliases = newColumnNameAliases;
        return this;
    }

    public DfJoin ifAbsent(Object valueIfAbsent)
    {
        return this.ifAbsent(Lists.fixedSize.of(valueIfAbsent));
    }

    public DfJoin ifAbsent(ListIterable<Object> newValuesIfAbsent)
    {
        this.valuesIfAbsent = newValuesIfAbsent;
        return this;
    }

    public DataFrame joinTo()
    {
        return this.joinTo;
    }

    public ListIterable<String> joinToColumnNames()
    {
        return this.joinToColumnNames;
    }

    public ListIterable<Object> valuesIfAbsent()
    {
        if (this.valuesIfAbsent == null)
        {
            this.valuesIfAbsent = Lists.mutable.of(new Object[this.selectFromJoined.size()]);
        }
        return this.valuesIfAbsent;
    }

    public ListIterable<String> selectFromJoined()
    {
        return this.selectFromJoined;
    }

    public ListIterable<String> columnNameAliases()
    {
        return this.columnNameAliases;
    }

    public ListIterable<String> columnsToLookup()
    {
        return this.columnsToLookup;
    }

    public DataFrame resolveLookup()
    {
        return this.joinFrom.lookup(this);
    }
}
