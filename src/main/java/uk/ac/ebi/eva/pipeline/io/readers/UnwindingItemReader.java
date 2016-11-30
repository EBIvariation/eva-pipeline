/*
 * Copyright 2016 EMBL - European Bioinformatics Institute
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package uk.ac.ebi.eva.pipeline.io.readers;

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

    private final ItemReader<? extends Collection<? extends T>> reader;
    private Iterator<? extends T> bufferIterator;

    public UnwindingItemReader(ItemReader<? extends Collection<? extends T>> reader) {
        this.reader = reader;
    }

    @Override
    public T read() throws Exception, UnexpectedInputException, ParseException, NonTransientResourceException {
        while (bufferIterator == null || !bufferIterator.hasNext()) {
            Collection<? extends T> buffer = reader.read();
            if (buffer == null) {
                return null;
            }
            this.bufferIterator = buffer.iterator();
        }
        return bufferIterator.next();
    }

    protected ItemReader<? extends Collection<? extends T>> getReader() {
        return reader;
    }
}