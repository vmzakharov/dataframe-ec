package io.github.vmzakharov.ecdataframe.dataframe;

import io.github.vmzakharov.ecdataframe.dsl.Expression;
import io.github.vmzakharov.ecdataframe.dsl.value.IntValue;
import io.github.vmzakharov.ecdataframe.dsl.value.Value;
import io.github.vmzakharov.ecdataframe.util.ExpressionParserHelper;

public class DfIntColumnComputed
extends DfIntColumn
implements DfColumnComputed
{
    private final String expressionAsString;
    private final Expression expression;

    public DfIntColumnComputed(DataFrame newDataFrame, String newName, String newExpressionAsString)
    {
        super(newDataFrame, newName);
        this.expressionAsString = newExpressionAsString;
        this.expression = ExpressionParserHelper.DEFAULT.toExpressionOrScript(this.expressionAsString);
    }

    @Override
    public int getInt(int rowIndex)
    {
        Value result = this.getValue(rowIndex);

        if (result.isVoid())
        {
            throw new NullPointerException("Null value at " + this.getName() + "[" + rowIndex + "]");
        }

        return ((IntValue) result).intValue();
    }

    @Override
    public Object getObject(int rowIndex)
    {
        Value result = this.getValue(rowIndex);

        return result.isVoid() ? null : ((IntValue) result).intValue();
    }

    @Override
    public boolean isNull(int rowIndex)
    {
        return this.getObject(rowIndex) == null;
    }

    @Override
    protected void addAllItemsFrom(DfIntColumn intColumn)
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
