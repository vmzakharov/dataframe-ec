package org.modelscript.expr.value;

import org.eclipse.collections.api.list.ListIterable;
import org.eclipse.collections.impl.factory.Lists;

public class VectorValue
implements Value
{
    public static final VectorValue EMPTY = new VectorValue(Lists.immutable.empty());

    private final ListIterable<Value> items;

    public VectorValue(ListIterable<Value> newItems)
    {
        this.items = newItems;
    }

    @Override
    public String asStringLiteral()
    {
        return this.items.collect(Value::asStringLiteral).makeString("[", ", ", "]");
    }

    @Override
    public ValueType getType()
    {
        return ValueType.VECTOR;
    }

    public ListIterable<Value> getItems()
    {
        return this.items;
    }

    public int size()
    {
        return this.items.size();
    }

    public Value get(int index)
    {
        return this.items.get(index);
    }

    @Override
    public int compareTo(Value o)
    {
        throw new UnsupportedOperationException("Not implemented");
    }
}
