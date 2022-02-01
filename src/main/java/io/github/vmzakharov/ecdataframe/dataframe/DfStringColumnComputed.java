package io.github.vmzakharov.ecdataframe.dataframe;

import io.github.vmzakharov.ecdataframe.dsl.DataFrameEvalContext;
import io.github.vmzakharov.ecdataframe.dsl.value.StringValue;
import io.github.vmzakharov.ecdataframe.dsl.visitor.InMemoryEvaluationVisitor;

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
        // todo: column in the variable expr or some other optimization?
        DataFrameEvalContext evalContext = this.getDataFrame().getEvalContext();
        evalContext.setRowIndex(rowIndex);

        StringValue result = (StringValue) this.getExpression().evaluate(new InMemoryEvaluationVisitor(evalContext));
        return result.stringValue();
    }
}
