package io.github.vmzakharov.ecdataframe.dataset;

public interface DataSet
{
    void openFileForReading();

    String getName();

    /**
     * Returns the next element in the data set.
     * @return the next element in the data set
     */
    Object next();

    boolean hasNext();

    void close();
}
