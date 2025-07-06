package io.github.vmzakharov.ecdataframe.dataframe;

import io.github.vmzakharov.ecdataframe.dsl.Expression;
import io.github.vmzakharov.ecdataframe.dsl.value.LongValue;
import io.github.vmzakharov.ecdataframe.dsl.value.Value;
import io.github.vmzakharov.ecdataframe.util.ExpressionParserHelper;

public class DfLongColumnComputed
extends DfLongColumn
implements DfColumnComputed
{
    private final String expressionAsString;
    private final Expression expression;

    public DfLongColumnComputed(DataFrame newDataFrame, String newName, String newExpressionAsString)
    {
        super(newDataFrame, newName);
        this.expressionAsString = newExpressionAsString;
        this.expression = ExpressionParserHelper.DEFAULT.toExpressionOrScript(this.expressionAsString);
    }

    @Override
    public long getLong(int rowIndex)
    {
        Value result = this.getValue(rowIndex);

        if (result.isVoid())
        {
            throw new NullPointerException("Null value at " + this.getName() + "[" + rowIndex + "]");
        }

        return ((LongValue) result).longValue();
    }

    @Override
    public Object getObject(int rowIndex)
    {
        Value result = this.getValue(rowIndex);

        return result.isVoid() ? null : ((LongValue) result).longValue();
    }

    @Override
    public boolean isNull(int rowIndex)
    {
        return this.getObject(rowIndex) == null;
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
