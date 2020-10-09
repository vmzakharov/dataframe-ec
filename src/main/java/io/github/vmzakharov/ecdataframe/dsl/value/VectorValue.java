package io.github.vmzakharov.ecdataframe.dsl.value;

import io.github.vmzakharov.ecdataframe.dsl.UnaryOp;
import org.eclipse.collections.api.list.ListIterable;
import org.eclipse.collections.impl.factory.Lists;

public class VectorValue
implements Value
{
    public static final VectorValue EMPTY = new VectorValue(Lists.immutable.empty());

    private final ListIterable<Value> elements;

    public VectorValue(ListIterable<Value> newItems)
    {
        this.elements = newItems;
    }

    @Override
    public String asStringLiteral()
    {
        return this.elements.collect(Value::asStringLiteral).makeString("(", ", ", ")");
    }

    @Override
    public ValueType getType()
    {
        return ValueType.VECTOR;
    }

    public ListIterable<Value> getElements()
    {
        return this.elements;
    }

    public int size()
    {
        return this.elements.size();
    }

    public Value get(int index)
    {
        return this.elements.get(index);
    }

    @Override
    public Value apply(UnaryOp operation)
    {
        return operation.applyVector(this.elements);
    }

    @Override
    public int compareTo(Value o)
    {
        throw new UnsupportedOperationException("Not implemented");
    }
}
