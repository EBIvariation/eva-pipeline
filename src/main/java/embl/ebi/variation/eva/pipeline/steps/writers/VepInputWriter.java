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
package embl.ebi.variation.eva.pipeline.steps.writers;

import embl.ebi.variation.eva.pipeline.annotation.generateInput.VariantWrapper;
import org.opencb.datastore.core.ObjectMap;
import org.springframework.batch.item.file.FlatFileItemWriter;
import org.springframework.batch.item.file.transform.BeanWrapperFieldExtractor;
import org.springframework.batch.item.file.transform.DelimitedLineAggregator;
import org.springframework.core.io.FileSystemResource;

/**
 * Flat file writer of the input file used by VEP
 *
 * The file is listing all the coordinates of variants and nucleotide changes like:
 *  20	60343	60343	G/A	+
 *  20	60419	60419	A/G	+
 *  20	60479	60479	C/T	+
 *  ...
 */

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
