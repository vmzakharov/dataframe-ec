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

public class FormatWithPlaceholders
{
    public static final String UNKNOWN_KEY_MESSAGE = "Unknown message key: \"%s\"";
    private static final MutableMap<String, String> MESSAGES_BY_KEY = Maps.mutable.of();
    private final String template;
    private final MutableMap<String, String> valuesByName = Maps.mutable.of();

    public FormatWithPlaceholders(String newTemplate)
    {
        this.template = newTemplate;
    }

    public static FormatWithPlaceholders message(String newTemplate)
    {
        return new FormatWithPlaceholders(newTemplate);
    }

    public static void addMessagesFromResourceBundle(ResourceBundle resourceBundle)
    {
        Enumeration<String> keys = resourceBundle.getKeys();
        while (keys.hasMoreElements())
        {
            String key = keys.nextElement();
            addMessage(key, resourceBundle.getString(key));
        }
    }

    public static void addMessage(String key, String message)
    {
        MESSAGES_BY_KEY.put(key, message);
    }

    public static FormatWithPlaceholders messageFromKey(String messageKey)
    {
        String message = MESSAGES_BY_KEY.getIfAbsent(messageKey, () -> String.format(UNKNOWN_KEY_MESSAGE, messageKey));
        return new FormatWithPlaceholders(message);
    }

    public static void addMessagesFromProperties(Properties properties)
    {
        properties.stringPropertyNames().forEach(name -> addMessage(name, properties.getProperty(name)));
    }

    public static void addMessagesFromMap(Map<String, String> newMessagesByKey)
    {
        MapIterate.forEachKeyValue(newMessagesByKey, FormatWithPlaceholders::addMessage);
    }

    public FormatWithPlaceholders with(String name, String value)
    {
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
