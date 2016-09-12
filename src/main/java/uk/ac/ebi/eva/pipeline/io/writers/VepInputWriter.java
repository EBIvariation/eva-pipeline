package uk.ac.ebi.eva.pipeline.io.writers;

import org.opencb.datastore.core.ObjectMap;
import org.springframework.batch.item.file.FlatFileItemWriter;
import org.springframework.batch.item.file.transform.BeanWrapperFieldExtractor;
import org.springframework.batch.item.file.transform.DelimitedLineAggregator;
import org.springframework.core.io.FileSystemResource;

import uk.ac.ebi.eva.pipeline.model.VariantWrapper;
import java.io.File;

public class VepInputWriter extends FlatFileItemWriter<VariantWrapper> {


    /**
 * @return must return a {@link FlatFileItemWriter} and not a {@link org.springframework.batch.item.ItemWriter}
 * {@see https://jira.spring.io/browse/BATCH-2097
 *
 * TODO: The variant list should be compressed
     * @param pipelineOptions
 */

    public VepInputWriter(File file) {
        super();

        BeanWrapperFieldExtractor<VariantWrapper> fieldExtractor = new BeanWrapperFieldExtractor<>();
        fieldExtractor.setNames(new String[] {"chr", "start", "end", "refAlt", "strand"});

        DelimitedLineAggregator<VariantWrapper> delLineAgg = new DelimitedLineAggregator<>();
        delLineAgg.setDelimiter("\t");
        delLineAgg.setFieldExtractor(fieldExtractor);

        setResource(new FileSystemResource(file));
        setAppendAllowed(false);
        setShouldDeleteIfExists(true);
        setLineAggregator(delLineAgg);
    }

    public VepInputWriter(String filePath){
        this(new File(filePath));
    }

}
