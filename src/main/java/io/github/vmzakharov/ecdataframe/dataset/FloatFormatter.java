package io.github.vmzakharov.ecdataframe.dataset;

import org.eclipse.collections.api.block.function.primitive.FloatFunction;
import org.eclipse.collections.impl.utility.StringIterate;

import java.text.DecimalFormat;
import java.text.ParseException;
import java.util.regex.Pattern;

import static io.github.vmzakharov.ecdataframe.util.ExceptionFactory.exceptionByKey;

public class FloatFormatter
{
    private DecimalFormat decimalFormat;
    private final FloatFunction<String> floatParser;
    private Pattern nonNumericPattern;

    public FloatFormatter(String pattern)
    {
        if (StringIterate.isEmpty(pattern))
        {
            this.floatParser = Float::parseFloat;
        }
        else if (pattern.length() == 1)
        {
            this.nonNumericPattern = Pattern.compile("[^0-9" + pattern + "]");
            this.floatParser = s -> Float.parseFloat(this.stripNonNumericCharacters(s).replace(pattern, "."));
        }
        else
        {
            this.decimalFormat = new DecimalFormat(pattern);
            this.floatParser = s -> {
                try
                {
                    return this.decimalFormat.parse(s.trim()).floatValue();
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

    public float parseAsFloat(String aString)
    {
        if (aString == null || aString.isEmpty())
        {
            return 0.0f;
        }

        return this.floatParser.floatValueOf(aString);
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
