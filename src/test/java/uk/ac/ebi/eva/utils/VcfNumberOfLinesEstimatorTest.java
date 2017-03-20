/*
 * Copyright 2017 EMBL - European Bioinformatics Institute
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
package uk.ac.ebi.eva.utils;

import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static uk.ac.ebi.eva.utils.FileUtils.getResource;

/**
 * Test for {@link VcfNumberOfLinesEstimator}
 */
public class VcfNumberOfLinesEstimatorTest {
    private static final String GENOTYPED_VCF = "/input-files/vcf/genotyped.vcf.gz";

    private static final String AGGREGATED_VCF = "/input-files/vcf/aggregated.vcf.gz";

    private static final String SMALL_VCF = "/input-files/vcf/small_genotyped.vcf.gz";

    private static final int PERCENTAGE_SIMILARITY = 20;

    private VcfNumberOfLinesEstimator numberOfLinesEstimator;

    @Before
    public void setUp() throws Exception {
        numberOfLinesEstimator = new VcfNumberOfLinesEstimator();
    }

    @Test
    public void predictedGenotypedVcfNumberOfLines() {
        String genotypedVcfPath = getResource(GENOTYPED_VCF).getAbsolutePath();
        long estimatedNumberOfLines = numberOfLinesEstimator.estimateVcfNumberOfLines(genotypedVcfPath);
        int expectedNumberOfLines = 298;

        assertEquals(expectedNumberOfLines, estimatedNumberOfLines,
                     (expectedNumberOfLines / 100) * PERCENTAGE_SIMILARITY);
    }

    @Test
    public void predictedAggregatedVcfNumberOfLines() {
        String aggregatedVcfPath = getResource(AGGREGATED_VCF).getAbsolutePath();
        long estimatedNumberOfLines = numberOfLinesEstimator.estimateVcfNumberOfLines(aggregatedVcfPath);
        int expectedNumberOfLines = 156;

        assertEquals(expectedNumberOfLines, estimatedNumberOfLines,
                     (expectedNumberOfLines / 100) * PERCENTAGE_SIMILARITY);
    }

    @Test
    public void smallVcfNumberOfLines() {
        String smallGenotypedVcfPath = getResource(SMALL_VCF).getAbsolutePath();
        long estimatedNumberOfLines = numberOfLinesEstimator.estimateVcfNumberOfLines(smallGenotypedVcfPath);

        assertEquals(0, estimatedNumberOfLines);
    }

}
