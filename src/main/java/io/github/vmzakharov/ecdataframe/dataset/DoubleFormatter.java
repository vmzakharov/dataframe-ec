package io.github.vmzakharov.ecdataframe.dataset;

import org.eclipse.collections.api.block.function.primitive.DoubleFunction;
import org.eclipse.collections.impl.utility.StringIterate;

import java.text.DecimalFormat;
import java.text.ParseException;
import java.util.regex.Pattern;

import static io.github.vmzakharov.ecdataframe.util.ExceptionFactory.exceptionByKey;

public class DoubleFormatter
{
    private DecimalFormat decimalFormat;
    private DoubleFunction<String> doubleParser;
    private Pattern nonNumericPattern;

    public DoubleFormatter(String pattern)
    {
        if (StringIterate.isEmpty(pattern))
        {
            this.doubleParser = Double::parseDouble;
        }
        else if (pattern.length() == 1)
        {
            this.nonNumericPattern = Pattern.compile("[^0-9" + pattern + "]");
            this.doubleParser = s -> Double.parseDouble(this.stripNonNumericCharacters(s).replace(pattern, "."));
        }
        else
        {
            this.decimalFormat = new DecimalFormat(pattern);
            this.doubleParser = s -> {
                try
                {
                    return this.decimalFormat.parse(s.trim()).doubleValue();
                }
                catch (ParseException e)
                {
                    throw exceptionByKey("CSV_PARSE_ERR")
                            .with("type", "a floating point number")
                            .with("inputString", s).get(e);
                }
            };
        }
    }

    public double parseAsDouble(String aString)
    {
        if (aString == null || aString.isEmpty())
        {
            return 0.0;
        }

        return this.doubleParser.applyAsDouble(aString);
    }

    private String stripNonNumericCharacters(String s)
    {
        return this.nonNumericPattern.matcher(s).replaceAll("");
    }

    public String format(double doubleValue)
    {
        return this.decimalFormat == null ? Double.toString(doubleValue) : this.decimalFormat.format(doubleValue);
    }
}
