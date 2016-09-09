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
package uk.ac.ebi.eva.pipeline.jobs.steps;

import com.mongodb.*;

import uk.ac.ebi.eva.pipeline.configuration.InitDBConfig;
import uk.ac.ebi.eva.pipeline.configuration.VariantJobsArgs;
import uk.ac.ebi.eva.pipeline.io.mappers.GeneLineMapper;
import uk.ac.ebi.eva.pipeline.io.readers.GeneReader;
import uk.ac.ebi.eva.pipeline.io.writers.GeneWriter;
import uk.ac.ebi.eva.pipeline.jobs.JobTestUtils;
import uk.ac.ebi.eva.pipeline.jobs.steps.processors.GeneFilterProcessor;
import uk.ac.ebi.eva.pipeline.model.FeatureCoordinates;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.test.MetaDataInstanceFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.io.Resource;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import uk.ac.evi.eva.test.data.GtfStaticTestData;

import java.io.*;
import java.util.*;
import java.util.zip.GZIPInputStream;

import static junit.framework.TestCase.*;
import static uk.ac.ebi.eva.pipeline.jobs.JobTestUtils.makeGzipFile;

/**
 * @author Jose Miguel Mut Lopez &lt;jmmut@ebi.ac.uk&gt;
 *
 * Test {@link GenesLoad}
 */
//@RunWith(SpringJUnit4ClassRunner.class)
//@ContextConfiguration(classes = { GenesLoad.class, InitDBConfig.class,})
public class GenesLoadTest {
//
//    private ItemProcessor<FeatureCoordinates, FeatureCoordinates> geneFilterProcessor;
//
//    @Autowired
//    public VariantJobsArgs variantJobsArgs;
//
//    private ExecutionContext executionContext;
//
//    @Before
//    public void setUp() throws Exception {
//        geneFilterProcessor =  new GeneFilterProcessor();
//        variantJobsArgs.loadArgs();
//        executionContext = MetaDataInstanceFactory.createStepExecution().getExecutionContext();
//    }

}
