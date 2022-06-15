package io.github.vmzakharov.ecdataframe.dsl.value;

import io.github.vmzakharov.ecdataframe.dsl.ArithmeticOp;
import io.github.vmzakharov.ecdataframe.dsl.Expression;
import io.github.vmzakharov.ecdataframe.dsl.PredicateOp;
import io.github.vmzakharov.ecdataframe.dsl.UnaryOp;
import io.github.vmzakharov.ecdataframe.dsl.visitor.ExpressionEvaluationVisitor;
import io.github.vmzakharov.ecdataframe.dsl.visitor.ExpressionVisitor;

public interface Value
extends Expression, Comparable<Value>
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

    @Override
    default Value evaluate(ExpressionEvaluationVisitor visitor)
    {
        return visitor.visitConstExpr(this);
    }

    @Override
    default void accept(ExpressionVisitor visitor)
    {
        visitor.visitConstExpr(this);
    }

    ValueType getType();

    default boolean isVoid()      { return this == VOID; }

    default boolean isBoolean()   { return this.getType().isBoolean(); }

    default boolean isLong()      { return this.getType().isLong(); }

    default boolean isDouble()    { return this.getType().isDouble(); }

    default boolean isNumber()    { return this.getType().isNumber(); }

    default boolean isString()    { return this.getType().isString(); }

    default boolean isDate()      { return this.getType().isDate(); }

    default boolean isTemporal()  { return this.getType().isTemporal(); }

    default boolean isDateTime()  { return this.getType().isDateTime(); }

    default boolean isVector()    { return this.getType().isVector(); }

    default boolean isDataFrame() { return this.getType().isString(); }
}

