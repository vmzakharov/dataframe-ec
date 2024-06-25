package io.github.vmzakharov.ecdataframe.dsl.value;

import io.github.vmzakharov.ecdataframe.dsl.UnaryOp;
import org.eclipse.collections.api.list.ListIterable;
import org.eclipse.collections.impl.factory.Lists;

public record VectorValue(ListIterable<Value> elements)
implements Value
{
    public static final VectorValue EMPTY = new VectorValue(Lists.immutable.empty());

    @Override
    public String asStringLiteral()
    {
        return this.elements.makeString(Value::asStringLiteral, "(", ", ", ")");
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
}
