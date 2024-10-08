package io.github.vmzakharov.ecdataframe.dsl.projection;

import io.github.vmzakharov.ecdataframe.dataframe.DataFrame;
import io.github.vmzakharov.ecdataframe.dataframe.DataFrameUtil;
import io.github.vmzakharov.ecdataframe.dataset.HierarchicalDataSet;
import io.github.vmzakharov.ecdataframe.dataset.ObjectListDataSet;
import io.github.vmzakharov.ecdataframe.dsl.EvalContext;
import io.github.vmzakharov.ecdataframe.dsl.ProjectionExpr;
import io.github.vmzakharov.ecdataframe.dsl.Script;
import io.github.vmzakharov.ecdataframe.dsl.SimpleEvalContext;
import io.github.vmzakharov.ecdataframe.dsl.value.DataFrameValue;
import io.github.vmzakharov.ecdataframe.dsl.value.Value;
import io.github.vmzakharov.ecdataframe.dsl.value.ValueType;
import io.github.vmzakharov.ecdataframe.dsl.visitor.InMemoryEvaluationVisitor;
import io.github.vmzakharov.ecdataframe.dsl.visitor.TypeInferenceVisitor;
import io.github.vmzakharov.ecdataframe.util.ExpressionParserHelper;
import org.eclipse.collections.api.factory.Lists;
import org.eclipse.collections.api.list.ListIterable;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;

import static io.github.vmzakharov.ecdataframe.dsl.value.ValueType.*;
import static io.github.vmzakharov.ecdataframe.dsl.value.ValueType.STRING;
import static org.junit.jupiter.api.Assertions.*;

public class ProjectionExtraTypesTest
{
    private HierarchicalDataSet dataSet;

    @BeforeEach
    public void initializeDataSet()
    {
        this.dataSet = new ObjectListDataSet("Donut", Lists.immutable.of(
                new Donut("Apple Cider", 1.25f, 600),
                new Donut("Blueberry",   1.10f, 500),
                new Donut("Glazed",      1.00f, 750),
                new Donut("Jelly",       1.50f, 800)
        ));
    }

    @Test
    public void readData()
    {
        this.dataSet.next();

        assertEquals("Apple Cider", this.dataSet.getValue("name"));
        assertEquals(1.25f, this.dataSet.getValue("price"));
        assertEquals(1.25, (Double) this.dataSet.getValue("priceAsDouble"), 0.000001);
        assertEquals(BigDecimal.valueOf(125, 2), this.dataSet.getValue("priceAsDecimal"));
        assertEquals(600, this.dataSet.getValue("calories"));
        assertEquals(600L, this.dataSet.getValue("caloriesAsLong"));
    }

    @Test
    public void typeInference()
    {
        String scriptString = """
                project {
                    Donut.name,
                    Donut.price,
                    Donut.calories,
                    Donut.priceAsDouble,
                    Donut.priceAsDecimal,
                    Donut.caloriesAsLong
                }""";

        EvalContext context = new SimpleEvalContext();
        context.addDataSet(this.dataSet);

        TypeInferenceVisitor visitor = new TypeInferenceVisitor(context);
        ProjectionExpr expr = (ProjectionExpr) ExpressionParserHelper.DEFAULT.toProjection(scriptString);

        ListIterable<ValueType> projectionExpressionTypes = expr.projectionExpressions().collect(visitor::inferExpressionType);

        assertEquals(Lists.immutable.of(STRING, FLOAT, INT, DOUBLE, DECIMAL, LONG), projectionExpressionTypes);
    }

    @Test
    public void simpleProjection()
    {
        String scriptString = """
                project {
                    name: Donut.name,
                    price: Donut.price,
                    calories: Donut.calories,
                    priceAsDouble: Donut.priceAsDouble,
                    priceAsDecimal: Donut.priceAsDecimal,
                    caloriesAsLong: Donut.caloriesAsLong
                }""";

        Script script = ExpressionParserHelper.DEFAULT.toScript(scriptString);

        InMemoryEvaluationVisitor visitor = new InMemoryEvaluationVisitor();
        visitor.getContext().addDataSet(this.dataSet);
        this.dataSet.openFileForReading(); // resets object iterator index

        Value result = script.evaluate(visitor);

        assertEquals(DataFrameValue.class, result.getClass());

        DataFrameUtil.assertEquals(
                new DataFrame("expected")
                        .addStringColumn("name").addFloatColumn("price").addIntColumn("calories")
                        .addDoubleColumn("priceAsDouble").addDecimalColumn("priceAsDecimal")
                        .addLongColumn("caloriesAsLong")
                        .addRow("Apple Cider", 1.25f, 600, 1.25, BigDecimal.valueOf(1.25), 600L)
                        .addRow("Blueberry",   1.10f, 500, 1.10, BigDecimal.valueOf(1.10), 500L)
                        .addRow("Glazed",      1.00f, 750, 1.00, BigDecimal.valueOf(1.00), 750L)
                        .addRow("Jelly",       1.50f, 800, 1.50, BigDecimal.valueOf(1.50), 800L)
                , ((DataFrameValue) result).dataFrame()
                , 0.000001
        );

    }
}
