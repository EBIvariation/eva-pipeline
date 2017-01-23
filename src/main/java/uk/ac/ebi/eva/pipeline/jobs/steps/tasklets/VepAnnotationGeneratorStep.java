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
package uk.ac.ebi.eva.pipeline.jobs.steps.tasklets;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.StepContribution;
import org.springframework.batch.core.scope.context.ChunkContext;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.repeat.RepeatStatus;
import org.springframework.beans.factory.annotation.Autowired;

import uk.ac.ebi.eva.pipeline.parameters.AnnotationParameter;

import java.io.BufferedInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Arrays;
import java.util.zip.GZIPOutputStream;

/**
 * Tasklet that runs @see <a href="http://www.ensembl.org/info/docs/tools/vep/index.html">VEP</a> over a list of
 * coordinates of variants and nucleotide changes to determines the effect of the mutations.
 * <p>
 * Input: file listing all the coordinates of variants and nucleotide changes like:
 * 20  60343  60343  G/A    +
 * 20  60419  60419  A/G    +
 * 20  60479  60479  C/T    +
 * ...
 * <p>
 * Output: file containing the VEP output
 * 20_60343_G/A    20:60343   A  -  -  -  intergenic_variant -  -  -  -  -  -
 * 20_60419_A/G    20:60419   G  -  -  -  intergenic_variant -  -  -  -  -  -
 * 20_60479_C/T    20:60479   T  -  -  -  intergenic_variant -  -  -  -  -  rs149529999    GMAF=T:0.0018;AFR_MAF=T:0.01;AMR_MAF=T:0.0028
 * ..
 */
public class VepAnnotationGeneratorStep implements Tasklet {
    private static final Logger logger = LoggerFactory.getLogger(VepAnnotationGeneratorStep.class);

    @Autowired
    AnnotationParameter annotationParameter;

    @Override
    public RepeatStatus execute(StepContribution contribution, ChunkContext chunkContext) throws Exception {

        ProcessBuilder processBuilder = new ProcessBuilder("perl", annotationParameter.getVepPath(),
                                                           "--cache",
                                                           "--cache_version", annotationParameter.getVepCacheVersion(),
                                                           "-dir", annotationParameter.getVepCachePath(),
                                                           "--species", annotationParameter.getVepCacheSpecies(),
                                                           "--fasta", annotationParameter.getInputFasta(),
                                                           "--fork", annotationParameter.getVepNumForks(),
                                                           "-i", annotationParameter.getVepInput(),
                                                           "-o", "STDOUT",
                                                           "--force_overwrite",
                                                           "--offline",
                                                           "--everything"
        );

        logger.debug("VEP annotation parameters = " + Arrays.toString(processBuilder.command().toArray()));

        logger.info("Starting read from VEP output");
        Process process = processBuilder.start();

        long written = connectStreams(new BufferedInputStream(process.getInputStream()),
                                      new GZIPOutputStream(new FileOutputStream(annotationParameter.getVepOuput())));

        int exitValue = process.waitFor();
        logger.info("Finishing read from VEP output, bytes written: " + written);

        if (exitValue > 0) {
            String errorLog = annotationParameter.getVepOuput() + ".errors.txt";
            connectStreams(new BufferedInputStream(process.getErrorStream()), new FileOutputStream(errorLog));
            throw new Exception("Error while running VEP (exit status " + exitValue + "). See "
                                        + errorLog + " for the errors description from VEP.");
        }

        return RepeatStatus.FINISHED;
    }

    /**
     * read all the inputStream and write it into the outputStream
     * <p>
     * TODO: optimize with buffers?
     *
     * @throws IOException
     */
    private long connectStreams(InputStream inputStream, OutputStream outputStream) throws IOException {
        int read = inputStream.read();
        long written = 0;
        while (read != -1) {
            written++;
            outputStream.write(read);
            read = inputStream.read();
        }

        outputStream.close();
        inputStream.close();
        return written;
    }
}
