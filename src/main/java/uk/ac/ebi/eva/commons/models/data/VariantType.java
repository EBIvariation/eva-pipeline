/*
 * Copyright 2018 EMBL - European Bioinformatics Institute
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
package uk.ac.ebi.eva.commons.models.data;

/**
 * Type of variation, per Sequence Ontology (SO) definitions, which depends mostly on its length.
 * See <a href="https://docs.google.com/spreadsheets/d/1YH8qDBDu7C6tqULJNCrGw8uBjdW3ZT5OjTkJzGNOZ4E/edit#gid=1433496764">Sequence Ontology definitions</a>,
 * <a href="http://www.sequenceontology.org/browser/current_release/term/SO:0001537">Structural variant</a>
 * and <a href="http://www.sequenceontology.org/browser/current_release/term/SO:0001019">Copy number variation</a>.
 */
public enum VariantType {

    /**
     * SO:0001483 - SNVs involve a single nucleotide, without changes in length
     */
    SNV,
    /**
     * SO:0000159 - DEL denotes deletions
     */
    DEL,
    /**
     * SO:0000667 - INS denotes insertions
     */
    INS,
    /**
     * SO:1000032 - Indels are insertions and deletions of less than SV_THRESHOLD (50) nucleotides
     */
    INDEL,
    /**
     * SO:0000705 - Short tandem repeats and microsatellites - ex: (A)3/(A)6
     */
    TANDEM_REPEAT,
    /**
     * SO:0001059 - Named variation - ex: (ALI06)
     */
    SEQUENCE_ALTERATION,
    /**
     * SO:0002073 - No variation was observed
     */
    NO_SEQUENCE_ALTERATION,
    /**
     * No alternate alleles found mean that only the reference was reported
     * @deprecated after <a href="https://www.ebi.ac.uk/panda/jira/browse/EVA-1220">decision to conform variant types to SO definitions</a>. Use NO_SEQUENCE_ALTERATION instead.
     */
    NO_ALTERNATE,
    /**
     * SO:0002007 - MNVs involve multiple nucleotides, without changes in length
     */
    MNV,

    /**
     * SO:0001537 - Structural variations are large changes of more than SV_THRESHOLD nucleotides
     */
    SV,
    /**
     * SO:0001019 - Copy-number variations alter the number of copies of a region
     */
    CNV
}
