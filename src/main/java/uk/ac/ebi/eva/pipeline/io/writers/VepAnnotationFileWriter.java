/*
 * Copyright 2015-2017 EMBL - European Bioinformatics Institute
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
package uk.ac.ebi.eva.pipeline.io.writers;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.item.ItemWriter;

import uk.ac.ebi.eva.pipeline.io.VepProcess;
import uk.ac.ebi.eva.pipeline.model.EnsemblVariant;
import uk.ac.ebi.eva.pipeline.parameters.AnnotationParameters;

import java.util.List;

/**
 * ItemStreamWriter that takes VariantWrappers and serialize them into a {@link VepProcess}, which will be responsible
 * for annotating the variants and writing them to a file.
 */
public class VepAnnotationFileWriter implements ItemWriter<EnsemblVariant> {

    private static final Logger logger = LoggerFactory.getLogger(VepAnnotationFileWriter.class);

    private final AnnotationParameters annotationParameters;

    private final Integer chunkSize;

    private final Long timeoutInSeconds;

    public VepAnnotationFileWriter(AnnotationParameters annotationParameters, Integer chunkSize, Long timeoutInSeconds) {
        this.annotationParameters = annotationParameters;
        this.chunkSize = chunkSize;
        this.timeoutInSeconds = timeoutInSeconds;
    }

    @Override
    public void write(List<? extends EnsemblVariant> variantWrappers) throws Exception {
        VepProcess vepProcess = new VepProcess(annotationParameters, chunkSize, timeoutInSeconds);
        vepProcess.open();

        for (EnsemblVariant ensemblVariant : variantWrappers) {
            String line = getVariantInVepInputFormat(ensemblVariant);
            vepProcess.write(line.getBytes());
            vepProcess.write(System.lineSeparator().getBytes());
        }

        if (variantWrappers.size() > 0) {
            EnsemblVariant first = variantWrappers.get(0);
            EnsemblVariant last = variantWrappers.get(variantWrappers.size() - 1);
            logger.trace("VEP has received {} variants from {}:{} to {}:{}", variantWrappers.size(),
                    first.getChr(), first.getStart(), last.getChr(), last.getStart());
        }

        vepProcess.flush();
        vepProcess.close();
    }

    private String getVariantInVepInputFormat(EnsemblVariant ensemblVariant) {
        return String.join("\t",
                           ensemblVariant.getChr(),
                           Integer.toString(ensemblVariant.getStart()),
                           Integer.toString(ensemblVariant.getEnd()),
                           ensemblVariant.getRefAlt(),
                           ensemblVariant.getStrand());
    }

}
