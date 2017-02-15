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
package uk.ac.ebi.eva.pipeline.parameters;

import org.opencb.datastore.core.ObjectMap;
import org.opencb.opencga.lib.common.Config;
import org.opencb.opencga.storage.core.variant.VariantStorageManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.io.IOException;

/**
 * Class to extract configuration from properties files and from command line.
 * Default values are in resources/application.properties
 * <p>
 * TODO: 20/05/2016 add type/null/file/dir validators
 * TODO validation checks for all the parameters
 */
@Component
public class JobOptions {
    private static final Logger logger = LoggerFactory.getLogger(JobOptions.class);

    @Value("${" + JobParametersNames.APP_OPENCGA_PATH + "}") private String opencgaAppHome;

    //// OpenCGA options with default values (non-customizable)
    private VariantStorageManager.IncludeSrc includeSourceLine = VariantStorageManager.IncludeSrc.FIRST_8_COLUMNS;

    // Skip steps
    @Value("${" + JobParametersNames.ANNOTATION_SKIP + ":false}") private boolean skipAnnot;
    @Value("${" + JobParametersNames.STATISTICS_SKIP + ":false}") private boolean skipStats;

    // Pipeline application options.
    @Value("${" + JobParametersNames.CONFIG_RESTARTABILITY_ALLOW + ":false}") private boolean allowStartIfComplete;
    @Value("${" + JobParametersNames.CONFIG_CHUNK_SIZE + ":1000}") private int chunkSize;

    private ObjectMap pipelineOptions = new ObjectMap();

    //These values are setted through VariantOptionsConfigurerListener
    private boolean calculateStats;
    private boolean includeStats;
    private boolean includeSamples;

    @PostConstruct
    public void loadArgs() throws IOException {
        logger.info("Loading job arguments");

        if (opencgaAppHome == null || opencgaAppHome.isEmpty()) {
            opencgaAppHome = System.getenv("OPENCGA_HOME") != null ? System.getenv("OPENCGA_HOME") : "/opt/opencga";
        }
        Config.setOpenCGAHome(opencgaAppHome);
        loadPipelineOptions();
    }

    private void loadPipelineOptions() {
        pipelineOptions.put(JobParametersNames.ANNOTATION_SKIP, skipAnnot);
        pipelineOptions.put(JobParametersNames.STATISTICS_SKIP, skipStats);
        logger.debug("Using as pipelineOptions: {}", pipelineOptions.entrySet().toString());
    }

    public void configureGenotypesStorage(boolean includeSamples) {
        this.includeSamples = includeSamples;
    }

    public void configureStatisticsStorage(boolean calculateStats, boolean includeStats) {
        this.calculateStats = calculateStats;
        this.includeStats = includeStats;
    }

    public ObjectMap getPipelineOptions() {
        return pipelineOptions;
    }

    public boolean isAllowStartIfComplete() {
        return allowStartIfComplete;
    }

    public int getChunkSize() {
        return chunkSize;
    }

    public boolean isCalculateStats() {
        return calculateStats;
    }

    public boolean isIncludeStats() {
        return includeStats;
    }

    public boolean isIncludeSamples() {
        return includeSamples;
    }

    public VariantStorageManager.IncludeSrc getIncludeSourceLine() {
        return includeSourceLine;
    }
}
