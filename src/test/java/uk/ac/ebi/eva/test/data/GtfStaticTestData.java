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
package uk.ac.ebi.eva.test.data;

public class GtfStaticTestData {

    public static final String GTF_CONTENT = "" +
            "#!genome-build ChlSab1.1\n" +
            "#!genome-version ChlSab1.1\n" +
            "#!genome-date 2014-03\n" +
            "#!genome-build-accession NCBI:GCA_000409795.2\n" +
            "#!genebuild-last-updated 2015-02\n" +
            "8\tensembl\tgene\t183180\t246703\t.\t+\t.\tgene_id \"ENSCSAG00000017073\"; gene_version \"1\"; gene_name \"FBXO25\"; gene_source \"ensembl\"; gene_biotype \"protein_coding\";\n" +
            "8\tensembl\ttranscript\t183180\t246703\t.\t+\t.\tgene_id \"ENSCSAG00000017073\"; gene_version \"1\"; transcript_id \"ENSCSAT00000015163\"; transcript_version \"1\"; gene_name \"FBXO25\"; gene_source \"ensembl\"; gene_biotype \"protein_coding\"; transcript_name \"FBXO25-201\"; transcript_source \"ensembl\"; transcript_biotype \"protein_coding\";\n" +
            "8\tensembl\texon\t183180\t183285\t.\t+\t.\tgene_id \"ENSCSAG00000017073\"; gene_version \"1\"; transcript_id \"ENSCSAT00000015163\"; transcript_version \"1\"; exon_number \"1\"; gene_name \"FBXO25\"; gene_source \"ensembl\"; gene_biotype \"protein_coding\"; transcript_name \"FBXO25-201\"; transcript_source \"ensembl\"; transcript_biotype \"protein_coding\"; exon_id \"ENSCSAE00000108645\"; exon_version \"1\";\n" +
            "8\tensembl\texon\t184547\t184671\t.\t+\t.\tgene_id \"ENSCSAG00000017073\"; gene_version \"1\"; transcript_id \"ENSCSAT00000015163\"; transcript_version \"1\"; exon_number \"2\"; gene_name \"FBXO25\"; gene_source \"ensembl\"; gene_biotype \"protein_coding\"; transcript_name \"FBXO25-201\"; transcript_source \"ensembl\"; transcript_biotype \"protein_coding\"; exon_id \"ENSCSAE00000108644\"; exon_version \"1\";\n" +
            "8\tensembl\tgene\t334894\t335006\t.\t+\t.\tgene_id \"ENSCSAG00000023576\"; gene_version \"1\"; gene_source \"ensembl\"; gene_biotype \"miRNA\";\n" +
            "8\tensembl\ttranscript\t334894\t335006\t.\t+\t.\tgene_id \"ENSCSAG00000023576\"; gene_version \"1\"; transcript_id \"ENSCSAT00000023666\"; transcript_version \"1\"; gene_source \"ensembl\"; gene_biotype \"miRNA\"; transcript_source \"ensembl\"; transcript_biotype \"miRNA\";\n" +
            "8\tensembl\texon\t334894\t335006\t.\t+\t.\tgene_id \"ENSCSAG00000023576\"; gene_version \"1\"; transcript_id \"ENSCSAT00000023666\"; transcript_version \"1\"; exon_number \"1\"; gene_source \"ensembl\"; gene_biotype \"miRNA\"; transcript_source \"ensembl\"; transcript_biotype \"miRNA\"; exon_id \"ENSCSAE00000192318\"; exon_version \"1\";\n";


    public static final String GTF_COMMENT_LINE = "#";
    public static final String GTF_LINE_SPLIT = "\n";
}
