package uk.ac.ebi.eva.pipeline.io.readers;

import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemStreamReader;
import org.springframework.batch.item.NonTransientResourceException;
import org.springframework.batch.item.ParseException;
import org.springframework.batch.item.UnexpectedInputException;

import java.util.Collection;
import java.util.Iterator;

/**
 * The unwinding reader takes a reader that returns a collection of elements in each read call and acts as a
 * buffer that pass single elements in each read call.
 *
 * @param <T>
 */
public class UnwindingItemStreamReader<T> extends UnwindingItemReader<T> implements ItemStreamReader<T> {

    public UnwindingItemStreamReader(ItemStreamReader<? extends Collection<? extends T>> windedReader) {
        super(windedReader);
    }

    @Override
    protected ItemStreamReader<? extends Collection<? extends T>> getWindedReader() {
        return (ItemStreamReader<? extends Collection<? extends T>>) super.getWindedReader();
    }

    @Override
    public void close() {
        getWindedReader().close();
    }

    @Override
    public void open(ExecutionContext executionContext) {
        getWindedReader().open(executionContext);
    }

    @Override
    public void update(ExecutionContext executionContext) {
        getWindedReader().update(executionContext);
    }

}
