package io.github.vmzakharov.ecdataframe.dsl.projection;

import io.github.vmzakharov.ecdataframe.dataset.HierarchicalDataSet;
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
import java.util.NoSuchElementException;

public class ObjectListDataSet
extends HierarchicalDataSet
{
    private final ListIterable<Object> items;
    private int index = 0;

    private final MutableListMultimap<String, MethodHandle> valueGetters = Multimaps.mutable.list.of();
    private final MutableMap<String, ValueType> valueTypes = Maps.mutable.of();

    public ObjectListDataSet(String newName, ListIterable<Object> newItems)
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

        throw new NoSuchElementException();
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

        if (getters.size() == 0)
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
                throw new RuntimeException(e);
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

            MethodHandle elementHandle;
            try
            {
                elementHandle = lookup.unreflect(elementMethod);
                currentClass = elementHandle.type().returnType();
                elementHandle.asType(elementHandle.type().changeReturnType(Object.class));
            }
            catch (IllegalAccessException e)
            {
                throw new RuntimeException(e);
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
            currentClass = elementMethod.getReturnType();
        }

        if (currentClass.equals(String.class))
        {
            return ValueType.STRING;
        }

        if (currentClass.equals(Number.class) || currentClass.equals(long.class))
        {
            return ValueType.LONG;
        }

        if (currentClass.equals(Double.class) || currentClass.equals(double.class))
        {
            return ValueType.DOUBLE;
        }

        return ValueType.VOID;
    }

    private Object firstObject()
    {
        if (this.items.size() == 0)
        {
            throw new NoSuchElementException("The data set contains no data");
        }

        return this.items.get(0);
    }

    private Object currentObject()
    {
        return this.items.get(this.index);
    }
}
