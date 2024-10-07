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

import java.time.LocalDate;
import java.time.LocalDateTime;

import static io.github.vmzakharov.ecdataframe.dsl.value.ValueType.DOUBLE;
import static io.github.vmzakharov.ecdataframe.dsl.value.ValueType.LONG;
import static io.github.vmzakharov.ecdataframe.dsl.value.ValueType.STRING;
import static org.junit.jupiter.api.Assertions.*;

public class HierarchicalDataSetProjectionTest
{
    private HierarchicalDataSet dataSet;

    @BeforeEach
    public void initializeDataSet()
    {
        this.dataSet = new ObjectListDataSet("Person", Lists.immutable.of(
                new Person("Alice", 123, 95.7, LocalDate.of(2022, 4, 3), "Lutefisk", LocalDateTime.of(1911, 11, 11, 11, 11, 11)),
                new Person("Bob", 456, 36.6, LocalDate.of(2022, 4, 2), "Chocolate", LocalDateTime.of(2022, 4, 6, 15, 24, 35))
        ));
    }

    @Test
    public void simpleDataSetOperations()
    {
        assertTrue(this.dataSet.hasNext());

        this.dataSet.next();

        assertEquals("Alice", this.dataSet.getValue("name"));
        assertEquals(123L, this.dataSet.getValue("luckyNumber"));
        assertEquals(95.7, this.dataSet.getValue("temperature"));

        assertTrue(this.dataSet.hasNext());

        this.dataSet.next();

        assertEquals("Bob", this.dataSet.getValue("name"));
        assertEquals(456L, this.dataSet.getValue("luckyNumber"));
        assertEquals(36.6, this.dataSet.getValue("temperature"));

        assertFalse(this.dataSet.hasNext());
    }

    @Test
    public void dataSetOperationsWithPropertyChain()
    {
        this.dataSet.next();

        assertEquals("Alice", this.dataSet.getValue("name"));
        assertEquals(123L, this.dataSet.getValue("luckyNumber"));
        assertEquals(123L, this.dataSet.getValue("bigLuckyNumber"));
        assertEquals(95.7, this.dataSet.getValue("temperature"));
        assertEquals(95.7, this.dataSet.getValue("bigTemperature"));
        assertEquals("Lutefisk", this.dataSet.getValue("favoriteFood.description"));

        this.dataSet.next();

        assertEquals("Bob", this.dataSet.getValue("name"));
        assertEquals(456L, this.dataSet.getValue("luckyNumber"));
        assertEquals(456L, this.dataSet.getValue("bigLuckyNumber"));
        assertEquals(36.6, this.dataSet.getValue("bigTemperature"));
        assertEquals(LocalDate.of(2022, 4, 2), this.dataSet.getValue("specialDate"));
        assertEquals("Chocolate", this.dataSet.getValue("favoriteFood.description"));
        assertEquals(LocalDateTime.of(2022, 4, 6, 15, 24, 35), this.dataSet.getValue("favoriteFood.lastEaten"));
    }

    @Test
    public void typeInference()
    {
        String scriptString =
                """
                project {
                    Person.name,
                    ${Lucky Number} : Person.luckyNumber,
                    Temp : Person.temperature,
                    Abc : "ABC"
                }""";

        EvalContext context = new SimpleEvalContext();
        context.addDataSet(this.dataSet);
        TypeInferenceVisitor visitor = new TypeInferenceVisitor(context);

        ProjectionExpr expr = (ProjectionExpr) ExpressionParserHelper.DEFAULT.toProjection(scriptString);

        ListIterable<ValueType> projectionExpressionTypes = expr.getProjectionExpressions().collect(visitor::inferExpressionType);

        assertEquals(Lists.immutable.of(STRING, LONG, DOUBLE, STRING), projectionExpressionTypes);
    }

    @Test
    public void simpleProjection()
    {
        String scriptString =
                """
                project {
                    Person.name,
                    ${Lucky Number} : Person.luckyNumber,
                    Temp : Person.temperature,
                    Abc : "ABC"
                }""";

        Script script = ExpressionParserHelper.DEFAULT.toScript(scriptString);

        InMemoryEvaluationVisitor visitor = new InMemoryEvaluationVisitor();
        visitor.getContext().addDataSet(this.dataSet);
        this.dataSet.openFileForReading();

        Value result = script.evaluate(visitor);

        assertEquals(DataFrameValue.class, result.getClass());

        DataFrameUtil.assertEquals(
                new DataFrame("expected")
                        .addStringColumn("Person.name").addLongColumn("Lucky Number").addDoubleColumn("Temp").addStringColumn("Abc")
                        .addRow("Alice", 123, 95.7, "ABC")
                        .addRow("Bob",   456, 36.6, "ABC"),
                ((DataFrameValue) result).dataFrame()
        );
    }

    @Test
    public void projectionWithBoxedNumbers()
    {
        String scriptString =
                """
                project {
                    Person.name,
                    ${Big Lucky Number} : Person.bigLuckyNumber,
                    BigTemp : Person.bigTemperature
                }""";

        Script script = ExpressionParserHelper.DEFAULT.toScript(scriptString);

        InMemoryEvaluationVisitor visitor = new InMemoryEvaluationVisitor();
        visitor.getContext().addDataSet(this.dataSet);
        this.dataSet.openFileForReading();

        Value result = script.evaluate(visitor);

        assertEquals(DataFrameValue.class, result.getClass());

        DataFrameUtil.assertEquals(
                new DataFrame("expected")
                        .addStringColumn("Person.name").addLongColumn("Big Lucky Number").addDoubleColumn("BigTemp")
                        .addRow("Alice", 123, 95.7)
                        .addRow("Bob",   456, 36.6),
                ((DataFrameValue) result).dataFrame()
        );
    }

    @Test
    public void nestedObjectProjection()
    {
        String scriptString =
                """
                project {
                    Person.name,
                    ${Lucky Number} : Person.luckyNumber,
                    Temp : Person.temperature,
                    Food : Person.favoriteFood.description
                } where Person.luckyNumber < 200""";

        Script script = ExpressionParserHelper.DEFAULT.toScript(scriptString);

        InMemoryEvaluationVisitor visitor = new InMemoryEvaluationVisitor();
        visitor.getContext().addDataSet(this.dataSet);
        this.dataSet.openFileForReading();

        Value result = script.evaluate(visitor);

        assertEquals(DataFrameValue.class, result.getClass());

        DataFrameUtil.assertEquals(
                new DataFrame("expected")
                        .addStringColumn("Person.name").addLongColumn("Lucky Number").addDoubleColumn("Temp").addStringColumn("Food")
                        .addRow("Alice", 123, 95.7, "Lutefisk"),
                ((DataFrameValue) result).dataFrame()
        );
    }

    @Test
    public void nestedObjectProjectionWithDates()
    {
        String scriptString =
                """
                project {
                    Person.name,
                    ${Lucky Number} : Person.luckyNumber,
                    Date : Person.specialDate,
                    FavoriteFood : Person.favoriteFood.description,
                    LastEaten : Person.favoriteFood.lastEaten,
                    LastEatenDateOnly : Person.favoriteFood.lastEaten.toLocalDate
                }""";

        Script script = ExpressionParserHelper.DEFAULT.toScript(scriptString);

        InMemoryEvaluationVisitor visitor = new InMemoryEvaluationVisitor();
        visitor.getContext().addDataSet(this.dataSet);
        this.dataSet.openFileForReading();

        Value result = script.evaluate(visitor);

        assertEquals(DataFrameValue.class, result.getClass());

        DataFrameUtil.assertEquals(
                new DataFrame("expected")
                        .addStringColumn("Person.name").addLongColumn("Lucky Number").addDateColumn("Date")
                        .addStringColumn("FavoriteFood").addDateTimeColumn("LastEaten").addDateColumn("LastEatenDateOnly")
                        .addRow("Alice", 123, LocalDate.of(2022, 4, 3), "Lutefisk",  LocalDateTime.of(1911, 11, 11, 11, 11, 11), LocalDate.of(1911, 11, 11))
                        .addRow("Bob",   456, LocalDate.of(2022, 4, 2), "Chocolate", LocalDateTime.of(2022, 4, 6, 15, 24, 35),   LocalDate.of(2022, 4, 6)),
                ((DataFrameValue) result).dataFrame()
        );
    }
}
