/*
 *
 * Copyright 2024 EMBL - European Bioinformatics Institute
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
 *
 */
package uk.ac.ebi.eva.pipeline.jobs.steps.processors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.item.ItemProcessor;
import uk.ac.ebi.eva.commons.core.models.pipeline.Variant;

import java.text.MessageFormat;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Direct implementation of VCF specification grammars from
 * <a href="https://github.com/EBIvariation/vcf-validator/blob/master/src/vcf/vcf.ragel">here</a> and
 * <a href="https://github.com/EBIvariation/vcf-validator/blob/master/src/vcf/vcf_v43.ragel">here</a>.
 */
public class ExcludeStructuralVariantsProcessor implements ItemProcessor<Variant, Variant> {

    private static final Logger logger = LoggerFactory.getLogger(ExcludeStructuralVariantsProcessor.class);

    private static String meta_contig_char = "(\\p{Alnum}|[\\p{Punct}&&[^:<>\\[\\]*=,]])";

    private static String chromBasicRegEx = MessageFormat.format("([{0}&&[^#]][{0}]*)", meta_contig_char);

    private static String chromContigRegEx = String.format("(<%s>)", chromBasicRegEx);

    private static String chromosomeRegEx = String.format("(%s|%s)", chromBasicRegEx, chromContigRegEx);

    private static String positionRegEx = "([\\p{Digit}]+)";

    private static String basesRegEx = "([ACTGNactgn]+)";

    private static String altIDRegEx_positive_match = "([\\p{Alnum}|[\\p{Punct}&&[^,<>]]]+)";

    private static String altIDRegEx_negative_match = "([\\p{Punct}]+)";

    private static String altIDRegEx = String.format("((?!%s)%s)", altIDRegEx_negative_match,
            altIDRegEx_positive_match);

    private static String stdPrefixRegEx = MessageFormat.format(
            "<DEL>|<INS>|<DUP>|<INV>|<CNV>|<DUP:TANDEM>|<DEL:ME:{0}>|<INS:ME:{0}>", "(\\p{Alnum})+");

    private static String altIndelRegEx = String.format("(%s|\\*)", stdPrefixRegEx);

    private static String altOtherRegEx = String.format("((?!%s)%s)", stdPrefixRegEx,
            String.format("<%s>", altIDRegEx));

    /**
     * See <a href="https://github.com/EBIvariation/vcf-validator/blob/be6cf8e2b35f2260166c1e6ffa1258a985a99ba3/src/vcf/vcf_v43.ragel#L190">VCF specification grammar</a>
     */
    private static String altSVRegEx = String.join("|",
            String.format("(\\]%s:%s\\]%s)", chromosomeRegEx, positionRegEx,
                    basesRegEx),
            String.format("(\\[%s:%s\\[%s)", chromosomeRegEx, positionRegEx,
                    basesRegEx),
            String.format("(%s\\]%s:%s\\])", basesRegEx, chromosomeRegEx,
                    positionRegEx),
            String.format("(%s\\[%s:%s\\[)", basesRegEx, chromosomeRegEx,
                    positionRegEx),
            String.format("(\\.%s)", basesRegEx),
            String.format("(%s\\.)", basesRegEx));

    private static String altGVCFRegEx = "(<\\*>)";

    /**
     * See <a href="https://github.com/EBIvariation/vcf-validator/blob/be6cf8e2b35f2260166c1e6ffa1258a985a99ba3/src/vcf/vcf_v43.ragel#L201">VCF specification grammar</a>
     */
    private static String STRUCTURAL_VARIANT_REGEX = String.format("^(%s|%s|%s|%s)$", altIndelRegEx, altSVRegEx,
            altGVCFRegEx, altOtherRegEx);

    private static final Pattern STRUCTURAL_VARIANT_PATTERN = Pattern.compile(STRUCTURAL_VARIANT_REGEX);

    @Override
    public Variant process(Variant variant) {
        Matcher matcher = STRUCTURAL_VARIANT_PATTERN.matcher(variant.getAlternate());
        if (matcher.matches()) {
            logger.info("Skipped processing structural variant " + variant);
            return null;
        }
        return variant;
    }
}
