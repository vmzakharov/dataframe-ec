package io.github.vmzakharov.ecdataframe.dataframe;

import io.github.vmzakharov.ecdataframe.dsl.Expression;
import io.github.vmzakharov.ecdataframe.dsl.value.BooleanValue;
import io.github.vmzakharov.ecdataframe.dsl.value.Value;
import io.github.vmzakharov.ecdataframe.util.ExpressionParserHelper;

public class DfBooleanColumnComputed
extends DfBooleanColumn
implements DfColumnComputed
{
    private final String expressionAsString;
    private final Expression expression;

    public DfBooleanColumnComputed(DataFrame newDataFrame, String newName, String newExpressionAsString)
    {
        super(newDataFrame, newName);
        this.expressionAsString = newExpressionAsString;
        this.expression = ExpressionParserHelper.DEFAULT.toExpressionOrScript(this.expressionAsString);
    }

    @Override
    public boolean getBoolean(int rowIndex)
    {
        Value result = this.getValue(rowIndex);

        if (result.isVoid())
        {
            throw new NullPointerException("Null value at " + this.getName() + "[" + rowIndex + "]");
        }

        return ((BooleanValue) result).isTrue();
    }

    @Override
    public Object getObject(int rowIndex)
    {
        Value result = this.getValue(rowIndex);

        return result.isVoid() ? null : ((BooleanValue) result).isTrue();
    }

    @Override
    public boolean isNull(int rowIndex)
    {
        return this.getObject(rowIndex) == null;
    }

    @Override
    protected void addAllItemsFrom(DfBooleanColumn booleanColumn)
    {
        this.throwUnmodifiableColumnException();
    }

    @Override
    public int getSize()
    {
        return this.getDataFrame().rowCount();
    }

    @Override
    public String getExpressionAsString()
    {
        return this.expressionAsString;
    }

    @Override
    public Expression getExpression()
    {
        return this.expression;
    }
}
