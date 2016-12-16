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
package uk.ac.ebi.eva.pipeline.jobs.steps.tasklets;

import org.opencb.biodata.formats.pedigree.io.PedigreePedReader;
import org.opencb.biodata.formats.pedigree.io.PedigreeReader;
import org.opencb.biodata.models.pedigree.Pedigree;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.StepContribution;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.core.scope.context.ChunkContext;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.repeat.RepeatStatus;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Import;
import org.springframework.stereotype.Component;

import uk.ac.ebi.eva.pipeline.parameters.JobOptions;
import uk.ac.ebi.eva.pipeline.parameters.JobParametersNames;

/**
 * Tasklet that parse and load a PED file into Mongo
 * <p>
 * PED specs
 * http://pngu.mgh.harvard.edu/~purcell/plink/data.shtml#ped
 * <p>
 * TODO: only reading for now, to be completed..
 */
public class PedLoaderStep implements Tasklet {
    private static final Logger logger = LoggerFactory.getLogger(PedLoaderStep.class);

    @Autowired
    private JobOptions jobOptions;

    private Pedigree pedigree;

    @Override
    public RepeatStatus execute(StepContribution contribution, ChunkContext chunkContext) throws Exception {
        PedigreeReader pedigreeReader = new PedigreePedReader(jobOptions.getPipelineOptions().getString(JobParametersNames.INPUT_PEDIGREE));
        pedigreeReader.open();
        pedigree = pedigreeReader.read().get(0);
        pedigreeReader.close();

        return RepeatStatus.FINISHED;
    }

    public Pedigree getPedigree() {
        return pedigree;
    }

}
