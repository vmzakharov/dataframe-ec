package io.github.vmzakharov.ecdataframe.util;

import org.eclipse.collections.api.factory.Maps;
import org.eclipse.collections.api.map.MutableMap;
import org.eclipse.collections.impl.utility.MapIterate;

import java.util.Enumeration;
import java.util.Map;
import java.util.Properties;
import java.util.ResourceBundle;
import java.util.function.Supplier;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static io.github.vmzakharov.ecdataframe.util.ErrorReporter.exception;

public class FormatWithPlaceholders
{
    private static MutableMap<String, String> messagesByKey = Maps.mutable.of();
    private final String template;
    private final MutableMap<String, String> valuesByName = Maps.mutable.of();

    public FormatWithPlaceholders(String newTemplate)
    {
        this.template = newTemplate;
    }

    public static FormatWithPlaceholders format(String newTemplate)
    {
        return new FormatWithPlaceholders(newTemplate);
    }

    public static void addMessagesFromResourceBundle(ResourceBundle resourceBundle)
    {
        Enumeration<String> keys = resourceBundle.getKeys();
        while (keys.hasMoreElements())
        {
            String key = keys.nextElement();
            addMessageByKey(key, resourceBundle.getString(key));
        }
    }

    static private void addMessageByKey(String key, String message)
    {
        messagesByKey.put(key, message);
    }

    public static FormatWithPlaceholders formatKey(String messageKey)
    {
        String message = messagesByKey.get(messageKey);
        return new FormatWithPlaceholders(message);
    }

    public static void addMessagesFromProperties(Properties properties)
    {
        properties.stringPropertyNames().forEach(name -> addMessageByKey(name,properties.getProperty(name)));
    }

    public static void addMessagesFromMap(Map<String, String> messagesByKey)
    {
        MapIterate.forEachKeyValue(messagesByKey, FormatWithPlaceholders::addMessageByKey);
    }

    public FormatWithPlaceholders with(String name, String value)
    {
        if (this.valuesByName.containsKey(name))
        {
            exception("Attempting to set error message template value twice for ${name}").with("name", name).fire();
        }
        this.valuesByName.put(name, value);
        return this;
    }

    @Override
    public String toString()
    {
        return this.substitute();
    }

    public Supplier<String> toStringSupplier()
    {
        return this::substitute;
    }

    private String substitute()
    {
        StringBuilder result = new StringBuilder();

        Pattern placeholderPattern = Pattern.compile("\\$\\{[\\w]++}");

        Matcher placeholderMatcher = placeholderPattern.matcher(this.template);

        int lastIndex = 0;
        while (placeholderMatcher.find())
        {
            String placeholder = placeholderMatcher.group(0);

            String key = placeholder.substring(2, placeholder.length() - 1);
            String value = this.valuesByName.getIfAbsentValue(key, "(" + key + " is unknown)");

            result
                    .append(this.template, lastIndex, placeholderMatcher.start(0))
                    .append(value);

            lastIndex = placeholderMatcher.end(0);
        }

        result.append(this.template.substring(lastIndex));

        return result.toString();
    }
}
