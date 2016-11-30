package uk.ac.ebi.eva.pipeline.io.readers;

import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemStreamReader;
import org.springframework.batch.item.NonTransientResourceException;
import org.springframework.batch.item.ParseException;
import org.springframework.batch.item.UnexpectedInputException;

import java.util.Collection;
import java.util.Iterator;

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