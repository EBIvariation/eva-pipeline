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

import org.opencb.opencga.lib.common.Config;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;

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

    @Value("${" + JobParametersNames.APP_OPENCGA_PATH + ":#{null}}") private String opencgaAppHome;

    // Pipeline application options.
    @Value("${" + JobParametersNames.CONFIG_RESTARTABILITY_ALLOW + ":false}") private boolean allowStartIfComplete;

    @PostConstruct
    public void loadArgs() {
        logger.info("Loading job arguments");

        if (opencgaAppHome == null || opencgaAppHome.isEmpty()) {
            opencgaAppHome = System.getenv("OPENCGA_HOME") != null ? System.getenv("OPENCGA_HOME") : "/opt/opencga";
        }
        Config.setOpenCGAHome(opencgaAppHome);
    }

    public boolean isAllowStartIfComplete() {
        return allowStartIfComplete;
    }

}
