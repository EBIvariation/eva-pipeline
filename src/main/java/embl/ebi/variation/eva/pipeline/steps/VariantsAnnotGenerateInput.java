/*
 * Copyright 2015-2016 EMBL - European Bioinformatics Institute
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
package embl.ebi.variation.eva.pipeline.steps;

import org.opencb.biodata.models.variant.Variant;
import org.opencb.biodata.models.variant.VariantSource;
import org.opencb.datastore.core.ObjectMap;
import org.opencb.datastore.core.QueryOptions;
import org.opencb.opencga.storage.core.StorageManagerFactory;
import org.opencb.opencga.storage.core.variant.VariantStorageManager;
import org.opencb.opencga.storage.core.variant.adaptors.VariantDBAdaptor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.StepContribution;
import org.springframework.batch.core.scope.context.ChunkContext;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.repeat.RepeatStatus;
import org.springframework.beans.factory.annotation.Autowired;

import java.io.BufferedOutputStream;
import java.io.FileOutputStream;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.util.Iterator;
import java.util.zip.GZIPOutputStream;

/**
 * Created by jmmut on 2015-12-09.
 *
 * @author Jose Miguel Mut Lopez &lt;jmmut@ebi.ac.uk&gt;
 */
public class VariantsAnnotGenerateInput implements Tasklet {
    private static final Logger logger = LoggerFactory.getLogger(VariantsAnnotGenerateInput.class);
    public static final String SKIP_ANNOT_GENERATE_INPUT = "skipAnnotGenerateInput";

    @Autowired
    private ObjectMap variantOptions;

    @Autowired
    private ObjectMap pipelineOptions;

    @Override
    public RepeatStatus execute(StepContribution contribution, ChunkContext chunkContext) throws Exception {

        if (pipelineOptions.getBoolean(SKIP_ANNOT_GENERATE_INPUT)) {
            logger.info("skipping annotation pre creation step, skipAnnotGenerateInput is set to {}",
                    pipelineOptions.getBoolean(SKIP_ANNOT_GENERATE_INPUT));
        } else {
            VariantStorageManager variantStorageManager = StorageManagerFactory.getVariantStorageManager();
            VariantSource variantSource = variantOptions.get(VariantStorageManager.VARIANT_SOURCE, VariantSource.class);
            VariantDBAdaptor dbAdaptor = variantStorageManager.getDBAdaptor(variantOptions.getString("dbName"), variantOptions);
            String vepInput = pipelineOptions.getString("vepInput");

            Writer writer = new OutputStreamWriter(new BufferedOutputStream(new GZIPOutputStream(new FileOutputStream(vepInput))));

            QueryOptions options = new QueryOptions(VariantDBAdaptor.ANNOTATION_EXISTS, false);
            options.add(VariantDBAdaptor.STUDIES, variantSource.getStudyId());
            options.add(VariantDBAdaptor.FILES, variantSource.getFileId());
            options.add("include", "chromosome,start,end,reference,alternative");

            Iterator<Variant> iterator = dbAdaptor.iterator(options);
            while(iterator.hasNext()) {
                Variant variant = iterator.next();
                writer.write(serializeVariant(variant));
            }

            writer.close();
        }

        return RepeatStatus.FINISHED;
    }

    /**
     * see http://www.ensembl.org/info/docs/tools/vep/vep_formats.html for an explanation of the format we are serializing here.
     */
    private String serializeVariant(Variant variant) {
        Variant formattedVariant = variant.copyInEnsemblFormat();
        return String.format("%s\t%s\t%s\t%s/%s\t+\n",
                formattedVariant.getChromosome(),
                formattedVariant.getStart(),
                formattedVariant.getEnd(),
                formattedVariant.getReference(),
                formattedVariant.getAlternate());
    }
}
