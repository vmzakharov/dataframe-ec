package io.github.vmzakharov.ecdataframe.dsl.value;

import io.github.vmzakharov.ecdataframe.dsl.ArithmeticOp;
import io.github.vmzakharov.ecdataframe.dsl.PredicateOp;
import io.github.vmzakharov.ecdataframe.dsl.UnaryOp;

public interface Value
extends Comparable<Value>
{
    Value VOID = new Value()
    {
        @Override
        public String asStringLiteral() { return "VOID"; }

        @Override
        public ValueType getType() { return ValueType.VOID; }

        @Override
        public int compareTo(Value other)
        {
            return this == other ? 0 : -1;
        }
    };

    default String stringValue()
    {
        return this.asStringLiteral();
    }

    String asStringLiteral();

    default Value apply(Value another, ArithmeticOp operation)
    {
        throw new UnsupportedOperationException("Undefined operation " + operation.asString() + " on " + this.asStringLiteral());
    }

    default Value apply(UnaryOp operation)
    {
        throw new UnsupportedOperationException("Undefined operation " + operation.asString() + " on " + this.asStringLiteral());
    }

    default BooleanValue applyPredicate(Value another, PredicateOp operation)
    {
        throw new UnsupportedOperationException("Undefined operation " + operation.asString() + " on " + this.asStringLiteral());
    }

    ValueType getType();

    default boolean isVoid()      { return this == VOID; }

    default boolean isBoolean()   { return this.getType().isBoolean(); }

    default boolean isLong()      { return this.getType().isLong(); }

    default boolean isDouble()    { return this.getType().isDouble(); }

    default boolean isNumber()    { return this.getType().isNumber(); }

    default boolean isString()    { return this.getType().isString(); }

    default boolean isDate()      { return this.getType().isDate(); }

    default boolean isDateTime()  { return this.getType().isDateTime(); }

    default boolean isVector()    { return this.getType().isVector(); }

    default boolean isDataFrame() { return this.getType().isString(); }
}

