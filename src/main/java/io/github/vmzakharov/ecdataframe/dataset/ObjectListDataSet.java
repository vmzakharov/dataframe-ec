package io.github.vmzakharov.ecdataframe.dataset;

import io.github.vmzakharov.ecdataframe.dsl.value.ValueType;
import org.eclipse.collections.api.factory.Maps;
import org.eclipse.collections.api.list.ListIterable;
import org.eclipse.collections.api.list.MutableList;
import org.eclipse.collections.api.map.MutableMap;
import org.eclipse.collections.api.multimap.list.MutableListMultimap;
import org.eclipse.collections.impl.factory.Lists;
import org.eclipse.collections.impl.factory.Multimaps;
import org.eclipse.collections.impl.utility.ArrayIterate;

import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.reflect.Method;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.chrono.ChronoLocalDate;
import java.time.chrono.ChronoLocalDateTime;

import static io.github.vmzakharov.ecdataframe.util.ExceptionFactory.exceptionByKey;

public class ObjectListDataSet
extends HierarchicalDataSet
{
    private final ListIterable<?> items;
    private int index = -1;

    private final MutableListMultimap<String, MethodHandle> valueGetters = Multimaps.mutable.list.of();
    private final MutableMap<String, ValueType> valueTypes = Maps.mutable.of();

    public ObjectListDataSet(String newName, ListIterable<?> newItems)
    {
        super(newName);
        this.items = newItems;
    }

    @Override
    public void openFileForReading()
    {
        this.index = -1;
    }

    @Override
    public Object next()
    {
        if (this.index < this.items.size())
        {
            this.index++;
            return this.items.get(this.index);
        }

        throw exceptionByKey("OBJ_END_OF_DATA_SET").with("dataSetName", this.getName()).get();
    }

    @Override
    public boolean hasNext()
    {
        return this.index < this.items.size() - 1;
    }

    @Override
    public void close()
    {
        // noop
    }

    @Override
    public Object getValue(String propertyChainString)
    {
        MutableList<MethodHandle> getters = this.valueGetters.get(propertyChainString);

        if (getters.isEmpty())
        {
            getters = this.valueGetters.getIfAbsentPutAll(propertyChainString, this.getGetters(propertyChainString));
        }

        Object currentValue = this.currentObject();
        for (int i = 0; i < getters.size(); i++)
        {
            MethodHandle methodHandle = getters.get(i);
            try
            {
                currentValue = methodHandle.invoke(currentValue);
            }
            catch (Throwable e)
            {
                exceptionByKey("OBJ_METHOD_INVOKE_FAIL").with("methodDetails", methodHandle).fire(e);
            }
        }
        return currentValue;
    }

    private MutableList<MethodHandle> getGetters(String propertyChainString)
    {
        MutableList<MethodHandle> getters = Lists.mutable.of();
        MethodHandles.Lookup lookup = MethodHandles.publicLookup();

        String[] elements = propertyChainString.split("\\.");

        Class<?> currentClass = this.currentObject().getClass();

        for (int i = 0; i < elements.length; i++)
        {
            String element = elements[i];

            Method elementMethod = ArrayIterate.detect(currentClass.getDeclaredMethods(), method -> method.getName().equals(element));

            if (elementMethod == null)
            {
                exceptionByKey("OBJ_PROPERTY_NOT_FOUND")
                        .with("property", element).with("className", currentClass.getName()).fire();
            }

            MethodHandle elementHandle;
            try
            {
                elementHandle = lookup.unreflect(elementMethod);
                currentClass = elementHandle.type().returnType();
                elementHandle.asType(elementHandle.type().changeReturnType(Object.class));
            }
            catch (IllegalAccessException e)
            {
                throw exceptionByKey("OBJ_PROPERTY_NOT_FOUND")
                        .with("property", element)
                        .with("className", currentClass.getName())
                        .get(e);
            }

            getters.add(elementHandle);
        }

        return getters;
    }

    @Override
    public ValueType getFieldType(String propertyChainString)
    {
        return this.valueTypes.getIfAbsentPut(propertyChainString, () -> this.resolveValueType(propertyChainString));
    }

    private ValueType resolveValueType(String propertyChainString)
    {
        String[] elements = propertyChainString.split("\\.");

        Class<?> currentClass = this.firstObject().getClass();

        for (int i = 0; i < elements.length; i++)
        {
            String element = elements[i];

            Method elementMethod = ArrayIterate.detect(currentClass.getDeclaredMethods(), method -> method.getName().equals(element));

            if (elementMethod == null)
            {
                exceptionByKey("OBJ_PROPERTY_NOT_FOUND")
                        .with("property", element).with("className", currentClass.getName()).fire();
            }

            currentClass = elementMethod.getReturnType();
        }

        if (currentClass.equals(String.class))
        {
            return ValueType.STRING;
        }

        if (currentClass.equals(long.class) || currentClass.equals(int.class)
            || currentClass.equals(Long.class) || currentClass.equals(Integer.class))
        {
            return ValueType.LONG;
        }

        if (currentClass.equals(double.class) || currentClass.equals(float.class)
            || currentClass.equals(Double.class) || currentClass.equals(Float.class))
        {
            return ValueType.DOUBLE;
        }

        if (currentClass.equals(LocalDate.class) || ChronoLocalDate.class.isAssignableFrom(currentClass))
        {
            return ValueType.DATE;
        }

        if (currentClass.equals(LocalDateTime.class) || ChronoLocalDateTime.class.isAssignableFrom(currentClass))
        {
            return ValueType.DATE_TIME;
        }

        return ValueType.VOID;
    }

    private Object firstObject()
    {
        if (this.items.isEmpty())
        {
            exceptionByKey("OBJ_EMPTY_DATA_SET_ACCESS").with("dataSetName", this.getName()).fire();
        }

        return this.items.getFirst();
    }

    private Object currentObject()
    {
        return this.items.get(this.index);
    }
}
