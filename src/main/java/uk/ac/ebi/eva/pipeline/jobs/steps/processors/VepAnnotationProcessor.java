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
package uk.ac.ebi.eva.pipeline.jobs.steps.processors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.item.ItemProcessor;

import uk.ac.ebi.eva.pipeline.io.VepProcess;
import uk.ac.ebi.eva.pipeline.model.EnsemblVariant;
import uk.ac.ebi.eva.pipeline.parameters.AnnotationParameters;

import java.io.StringWriter;
import java.util.Arrays;
import java.util.List;

/**
 * ItemStreamWriter that takes VariantWrappers and serialize them into a {@link VepProcess}, which will be responsible
 * for annotating the variants and writing them to a file.
 */
public class VepAnnotationProcessor implements ItemProcessor<List<EnsemblVariant>, List<String>> {

    private static final Logger logger = LoggerFactory.getLogger(VepAnnotationProcessor.class);

    private static final boolean SKIP_COMMENTS = true;

    private final AnnotationParameters annotationParameters;

    private final Integer chunkSize;

    private final Long timeoutInSeconds;

    public VepAnnotationProcessor(AnnotationParameters annotationParameters, Integer chunkSize, Long timeoutInSeconds) {
        this.annotationParameters = annotationParameters;
        this.chunkSize = chunkSize;
        this.timeoutInSeconds = timeoutInSeconds;
    }

    @Override
    public List<String> process(List<EnsemblVariant> ensemblVariants) throws Exception {
        StringWriter writer = new StringWriter();

        VepProcess vepProcess = new VepProcess(annotationParameters, chunkSize, timeoutInSeconds, writer, SKIP_COMMENTS);
        vepProcess.open();

        for (EnsemblVariant ensemblVariant : ensemblVariants) {
            String line = getVariantInVepInputFormat(ensemblVariant);
            vepProcess.write(line.getBytes());
            vepProcess.write(System.lineSeparator().getBytes());
        }

        logBatch(ensemblVariants);

        vepProcess.flush();
        vepProcess.close();
        writer.close();

        String fullVEPOutput = writer.getBuffer().toString();
        String[] lines = fullVEPOutput.split("\n"); // TODO is it possible to refactor this?
        return fullVEPOutput.trim().equals("")? null : Arrays.asList(lines);
    }

    private void logBatch(List<EnsemblVariant> ensemblVariants) {
        if (ensemblVariants.size() > 0) {
            EnsemblVariant first = ensemblVariants.get(0);
            EnsemblVariant last = ensemblVariants.get(ensemblVariants.size() - 1);
            logger.trace("VEP has received {} variants from {}:{} to {}:{}", ensemblVariants.size(),
                    first.getChr(), first.getStart(), last.getChr(), last.getStart());
        }
    }

    private String getVariantInVepInputFormat(EnsemblVariant ensemblVariant) {
        return String.join("\t",
                           ensemblVariant.getChr(),
                           Long.toString(ensemblVariant.getStart()),
                           Long.toString(ensemblVariant.getEnd()),
                           ensemblVariant.getRefAlt(),
                           ensemblVariant.getStrand());
    }
}
