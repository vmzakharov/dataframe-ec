package io.github.vmzakharov.ecdataframe.dsl;

import io.github.vmzakharov.ecdataframe.dsl.value.Value;
import io.github.vmzakharov.ecdataframe.dsl.visitor.ExpressionEvaluationVisitor;
import io.github.vmzakharov.ecdataframe.dsl.visitor.ExpressionVisitor;

public class IndexExpr
implements Expression
{

    private final Expression vectorExpr;
    private final Expression indexExpr;

    public IndexExpr(Expression newVectorExpr, Expression newIndexExpr)
    {
        this.vectorExpr = newVectorExpr;
        this.indexExpr = newIndexExpr;
    }

    public Expression getVectorExpr()
    {
        return this.vectorExpr;
    }

    public Expression getIndexExpr()
    {
        return this.indexExpr;
    }

    @Override
    public Value evaluate(ExpressionEvaluationVisitor evaluationVisitor)
    {
        return evaluationVisitor.visitIndexExpr(this);
    }

    @Override
    public void accept(ExpressionVisitor visitor)
    {
        visitor.visitIndexExpr(this);
    }
}
