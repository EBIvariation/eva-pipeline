package uk.ac.ebi.eva.pipeline.steps.writers;

import org.opencb.datastore.core.ObjectMap;
import org.springframework.batch.item.file.FlatFileItemWriter;
import org.springframework.batch.item.file.transform.BeanWrapperFieldExtractor;
import org.springframework.batch.item.file.transform.DelimitedLineAggregator;
import org.springframework.core.io.FileSystemResource;

import uk.ac.ebi.eva.pipeline.annotation.generateInput.VariantWrapper;

public class VepInputWriter extends FlatFileItemWriter<VariantWrapper> {


    /**
 * @return must return a {@link FlatFileItemWriter} and not a {@link org.springframework.batch.item.ItemWriter}
 * {@see https://jira.spring.io/browse/BATCH-2097
 *
 * TODO: The variant list should be compressed
     * @param pipelineOptions
 */

    public VepInputWriter(ObjectMap pipelineOptions) {
        super();

        BeanWrapperFieldExtractor<VariantWrapper> fieldExtractor = new BeanWrapperFieldExtractor<>();
        fieldExtractor.setNames(new String[] {"chr", "start", "end", "refAlt", "strand"});

        DelimitedLineAggregator<VariantWrapper> delLineAgg = new DelimitedLineAggregator<>();
        delLineAgg.setDelimiter("\t");
        delLineAgg.setFieldExtractor(fieldExtractor);

        setResource(new FileSystemResource(pipelineOptions.getString("vep.input")));
        setAppendAllowed(false);
        setShouldDeleteIfExists(true);
        setLineAggregator(delLineAgg);
    }

}
