package io.github.vmzakharov.ecdataframe.dataframe;

import io.github.vmzakharov.ecdataframe.dsl.DataFrameEvalContext;
import io.github.vmzakharov.ecdataframe.dsl.Expression;
import io.github.vmzakharov.ecdataframe.dsl.value.StringValue;
import io.github.vmzakharov.ecdataframe.dsl.visitor.InMemoryEvaluationVisitor;
import io.github.vmzakharov.ecdataframe.util.ExpressionParserHelper;
import org.eclipse.collections.api.list.ImmutableList;
import org.eclipse.collections.api.list.ListIterable;
import org.eclipse.collections.impl.factory.Lists;
import org.eclipse.collections.impl.list.Interval;

public class DfStringColumnComputed
extends DfObjectColumnComputed<String>
implements DfStringColumn
{
    public DfStringColumnComputed(DataFrame newDataFrame, String newName, String newExpressionAsString)
    {
        super(newDataFrame, newName, newExpressionAsString);
    }

    @Override
    public String getTypedObject(int rowIndex)
    {
        return this.getString(rowIndex);
    }

    @Override
    public String getString(int rowIndex)
    {
        // todo: column in the variable expr or some other optimization?
        DataFrameEvalContext evalContext = this.getDataFrame().getEvalContext();
        evalContext.setRowIndex(rowIndex);

        StringValue result = (StringValue) this.getExpression().evaluate(new InMemoryEvaluationVisitor(evalContext));
        return result.stringValue();
    }
}
