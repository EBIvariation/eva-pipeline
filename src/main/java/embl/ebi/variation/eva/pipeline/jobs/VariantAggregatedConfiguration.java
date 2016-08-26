/*
 * Copyright 2015 EMBL - European Bioinformatics Institute
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
package embl.ebi.variation.eva.pipeline.jobs;

import embl.ebi.variation.eva.pipeline.steps.VariantsLoad;
import embl.ebi.variation.eva.pipeline.steps.VariantsTransform;
import org.opencb.datastore.core.ObjectMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.core.step.builder.TaskletStepBuilder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.core.env.Environment;

import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Paths;

@Configuration
@EnableBatchProcessing
@Import({VariantJobArgsConfig.class, VariantsLoad.class, VariantsTransform.class})
public class VariantAggregatedConfiguration extends CommonJobStepInitialization{

    private static final Logger logger = LoggerFactory.getLogger(VariantAggregatedConfiguration.class);
    public static final String jobName = "load-aggregated-vcf";

    @Autowired
    private JobBuilderFactory jobBuilderFactory;
    @Autowired
    private StepBuilderFactory stepBuilderFactory;

    @Autowired
    private VariantsLoad variantsLoad;
    @Autowired
    private VariantsTransform variantsTransform;

    @Bean
    public Job aggregatedVariantJob() {
        JobBuilder jobBuilder = jobBuilderFactory
                .get(jobName)
                .incrementer(new RunIdIncrementer());

        return jobBuilder
                .start(transform())
                .next(load())
//                .next(statsCreate())
//                .next(statsLoad())
//                .next(annotation(stepBuilderFactory));
                .build();
    }

    private Step transform() {
        StepBuilder step1 = stepBuilderFactory.get("Normalize variants");
        final TaskletStepBuilder tasklet = step1.tasklet(variantsTransform);
        initStep(tasklet);
        return tasklet.build();
    }

    private Step load() {
        StepBuilder step1 = stepBuilderFactory.get("Load variants");
        TaskletStepBuilder tasklet = step1.tasklet(variantsLoad);
        initStep(tasklet);
        return tasklet.build();
    }

/*
    @Autowired
    JobLauncher jobLauncher;
    @Autowired
    JobExplorer jobExplorer;
    @Autowired
    JobRepository jobRepository;
    @Autowired
    JobRegistry jobRegistry;


    @Value("${input}")
    private String input;

    @Bean
    public Job AggregatedVariantJob(JobBuilderFactory jobs, JobExecutionListener listener, StepBuilderFactory stepBuilderFactory) {
        JobBuilder jobBuilder = jobs.get(jobName)
                .repository(jobRepository)
                .incrementer(new RunIdIncrementer())
                .listener(listener);

        JobFlowBuilder flow = jobBuilder.flow(transform(stepBuilderFactory));

        return flow.end().build();
    }

    @Bean
    public Step transform(StepBuilderFactory stepBuilderFactory) {
        StepBuilder step1 = stepBuilderFactory.get("transform");
        TaskletStepBuilder tasklet = step1.tasklet(new Tasklet() {
            @Override
            public RepeatStatus execute(StepContribution contribution, ChunkContext chunkContext) throws Exception {
                log.info("transform mock, aggregated file " + input);
                return RepeatStatus.FINISHED;
            }
        });

        // true: every job execution will do this step, even if it is COMPLETED
        // false: if the job was aborted and is relaunched, this step will NOT be done again
        tasklet.allowStartIfComplete(false);

        return tasklet.build();
    }
    */

}
