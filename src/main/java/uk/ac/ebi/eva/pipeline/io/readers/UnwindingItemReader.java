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
 * buffer that pass single elements in each read call. This class can be used with any kind of item reader,
 * beware that some subtypes of ItemReaders like {@link ItemStreamReader} require special operations before
 * and after read operations. If your reader extends {@link ItemStreamReader} please use
 * {@link UnwindingItemStreamReader}
 *
 * @param <T>
 */
public class UnwindingItemReader <T> implements ItemReader<T> {

    private final ItemReader<? extends Collection<? extends T>> windedReader;
    private Iterator<? extends T> bufferIterator;

    public UnwindingItemReader(ItemReader<? extends Collection<? extends T>> windedReader) {
        this.windedReader = windedReader;
    }

    @Override
    public T read() throws Exception, UnexpectedInputException, ParseException, NonTransientResourceException {
        while (bufferIterator == null || !bufferIterator.hasNext()) {
            Collection<? extends T> buffer = windedReader.read();
            if (buffer == null) {
                return null;
            }
            this.bufferIterator = buffer.iterator();
        }
        return bufferIterator.next();
    }

    protected ItemReader<? extends Collection<? extends T>> getWindedReader() {
        return windedReader;
    }
}