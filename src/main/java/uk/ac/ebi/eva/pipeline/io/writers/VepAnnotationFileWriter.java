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
import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.ItemStreamException;
import org.springframework.batch.item.ItemStreamWriter;

import uk.ac.ebi.eva.pipeline.io.VepProcess;
import uk.ac.ebi.eva.pipeline.model.VariantWrapper;
import uk.ac.ebi.eva.pipeline.parameters.AnnotationParameters;

import java.util.List;

/**
 * ItemStreamWriter that takes VariantWrappers and serialize them into a {@link VepProcess}, which will take care of
 * annotate the variants and write them to a file.
 */
public class VepAnnotationFileWriter implements ItemStreamWriter<VariantWrapper> {

    private static final Logger logger = LoggerFactory.getLogger(VepAnnotationFileWriter.class);

    private final VepProcess vepProcess;

    public VepAnnotationFileWriter(AnnotationParameters annotationParameters, Integer chunkSize, Long timeoutInSeconds) {
        vepProcess = new VepProcess(annotationParameters, chunkSize, timeoutInSeconds);
    }

    @Override
    public void open(ExecutionContext executionContext) throws ItemStreamException {
        // don't open: we need lazy opening because vep fails if there is nothing to write
    }


    @Override
    public void write(List<? extends VariantWrapper> variantWrappers) throws Exception {
        if (!vepProcess.isOpen()) {
            vepProcess.open();
        }

        for (VariantWrapper variantWrapper : variantWrappers) {
            String line = getVariantInVepInputFormat(variantWrapper);
            vepProcess.write(line.getBytes());
            vepProcess.write(System.lineSeparator().getBytes());
        }

        if (variantWrappers.size() > 0) {
            VariantWrapper first = variantWrappers.get(0);
            VariantWrapper last = variantWrappers.get(variantWrappers.size() - 1);
            logger.debug("VEP has received {} variants from {}:{} to {}:{}", variantWrappers.size(),
                    first.getChr(), first.getStart(), last.getChr(), last.getStart());
        }

        vepProcess.flush();
    }

    private String getVariantInVepInputFormat(VariantWrapper variantWrapper) {
        return String.join("\t",
                variantWrapper.getChr(),
                Integer.toString(variantWrapper.getStart()),
                Integer.toString(variantWrapper.getEnd()),
                variantWrapper.getRefAlt(),
                variantWrapper.getStrand());
    }

    @Override
    public void update(ExecutionContext executionContext) throws ItemStreamException {

    }

    @Override
    public void close() throws ItemStreamException {
        vepProcess.close();
    }

}
