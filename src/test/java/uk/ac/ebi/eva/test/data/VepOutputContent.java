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

public class VepOutputContent {

    public static final String vepOutputContent = "" +
            "20_63351_A/G\t20:63351\tG\tENSG00000178591\tENST00000608838\tTranscript\tupstream_gene_variant\t-\t-\t-\t-\t-\trs181305519\tDISTANCE=4540;STRAND=1;SYMBOL=DEFB125;SYMBOL_SOURCE=HGNC;HGNC_ID=18105;BIOTYPE=processed_transcript;GMAF=G:0.0005;AFR_MAF=G:0.0020;polyphen=possibly_damaging(0.859);sift=tolerated(0.07);\n" +
            "20_63360_C/T\t20:63360\tT\tENSG00000178591\tENST00000382410\tTranscript\tupstream_gene_variant\t-\t-\t-\t-\t-\trs186156309\tDISTANCE=4991;STRAND=1;SYMBOL=DEFB125;SYMBOL_SOURCE=HGNC;HGNC_ID=18105;BIOTYPE=protein_coding;CANONICAL=YES;CCDS=CCDS12989.2;ENSP=ENSP00000371847;SWISSPROT=DB125_HUMAN;TREMBL=B2R4E8_HUMAN;UNIPARC=UPI00001A36DE;GMAF=T:0.0014;AMR_MAF=T:0.01\n" +
            "20_63360_C/T\t20:63360\tT\tENSG00000178591\tENST00000608838\tTranscript\tupstream_gene_variant\t-\t-\t-\t-\t-\trs186156309\tDISTANCE=4531;STRAND=1;SYMBOL=DEFB125;SYMBOL_SOURCE=HGNC;HGNC_ID=18105;BIOTYPE=processed_transcript;GMAF=T:0.0014;AMR_MAF=T:0.01\n" +
            "20_63399_G/A\t20:63399\tA\tENSG00000178591\tENST00000382410\tTranscript\tupstream_gene_variant\t-\t-\t-\t-\t-\t-\tDISTANCE=4952;STRAND=1;SYMBOL=DEFB125;SYMBOL_SOURCE=HGNC;HGNC_ID=18105;BIOTYPE=protein_coding;CANONICAL=YES;CCDS=CCDS12989.2;ENSP=ENSP00000371847;SWISSPROT=DB125_HUMAN;TREMBL=B2R4E8_HUMAN;UNIPARC=UPI00001A36DE\n" +
            "20_63399_G/A\t20:63399\tA\tENSG00000178591\tENST00000608838\tTranscript\tupstream_gene_variant\t-\t-\t-\t-\t-\t-\tDISTANCE=4492;STRAND=1;SYMBOL=DEFB125;SYMBOL_SOURCE=HGNC;HGNC_ID=18105;BIOTYPE=processed_transcript\n" +
            "20_63426_G/T\t20:63426\tT\tENSG00000178591\tENST00000382410\tTranscript\tupstream_gene_variant\t-\t-\t-\t-\t-\trs147063585\tDISTANCE=4925;STRAND=1;SYMBOL=DEFB125;SYMBOL_SOURCE=HGNC;HGNC_ID=18105;BIOTYPE=protein_coding;CANONICAL=YES;CCDS=CCDS12989.2;ENSP=ENSP00000371847;SWISSPROT=DB125_HUMAN;TREMBL=B2R4E8_HUMAN;UNIPARC=UPI00001A36DE;GMAF=T:0.0028;AFR_MAF=T:0.01\n" +
            "20_63426_G/T\t20:63426\tT\tENSG00000178591\tENST00000608838\tTranscript\tupstream_gene_variant\t-\t-\t-\t-\t-\trs147063585\tDISTANCE=4465;STRAND=1;SYMBOL=DEFB125;SYMBOL_SOURCE=HGNC;HGNC_ID=18105;BIOTYPE=processed_transcript;GMAF=T:0.0028;AFR_MAF=T:0.01\n";

    public static final String vepOutputContentMalformedVariantFields = "" +
            "20_63351_AG\t20:63351\tG\tENSG00000178591\tENST00000608838\tTranscript\tupstream_gene_variant\t-\t-\t-\t-\t-\trs181305519\tDISTANCE=4540;STRAND=1;SYMBOL=DEFB125;SYMBOL_SOURCE=HGNC;HGNC_ID=18105;BIOTYPE=processed_transcript;GMAF=G:0.0005;AFR_MAF=G:0.0020\n";

    public static final String vepOutputContentMalformedCoordinates = "" +
            "20_63351_A/G\t20_63351\tG\tENSG00000178591\tENST00000608838\tTranscript\tupstream_gene_variant\t-\t-\t-\t-\t-\trs181305519\tDISTANCE=4540;STRAND=1;SYMBOL=DEFB125;SYMBOL_SOURCE=HGNC;HGNC_ID=18105;BIOTYPE=processed_transcript;GMAF=G:0.0005;AFR_MAF=G:0.0020\n";

    public static final String vepOutputContentTranscriptFields = "" +
            "20_63351_A/G\t20:63351\tG\tENSG00000178591\tENST00000608838\tTranscript\tupstream_gene_variant\t1-2\t?-4\t?-?\t7-?\t9-10\trs181305519\tDISTANCE=4540;STRAND=1;SYMBOL=DEFB125;SYMBOL_SOURCE=HGNC;HGNC_ID=18105;BIOTYPE=processed_transcript;GMAF=G:0.0005;AFR_MAF=G:0.0020\n";

    public static final String vepOutputContentWithOutTranscript = "" +
            "20_63351_A/G\t20:63351\tG\tENSG00000178591\tENST00000608838\t-\tupstream_gene_variant\t-\t-\t-\t-\t-\trs181305519\tDISTANCE=4540;STRAND=1;SYMBOL=DEFB125;SYMBOL_SOURCE=HGNC;HGNC_ID=18105;BIOTYPE=processed_transcript;GMAF=G:0.0005;AFR_MAF=G:0.0020\n";

    public static final String vepOutputContentChromosomeIdWithUnderscore = "" +
               "20_1_63351_A/G\t20:63351\tG\tENSG00000178591\tENST00000608838\tTranscript\tupstream_gene_variant\t-\t-\t-\t-\t-\trs181305519\tDISTANCE=4540;STRAND=1;SYMBOL=DEFB125;SYMBOL_SOURCE=HGNC;HGNC_ID=18105;BIOTYPE=processed_transcript;GMAF=G:0.0005;AFR_MAF=G:0.0020\n";

    public static final String vepOutputContentWithExtraFieldsSingleAnnotation = "" +
            "20_63351_A/G\t20:63351\tG\tENSG00000178591\tENST00000608838\tTranscript\tupstream_gene_variant\t-\t-\t-\t-\t-\trs181305519\tDISTANCE=4540;STRAND=1;SYMBOL=DEFB125;SYMBOL_SOURCE=HGNC;HGNC_ID=18105;BIOTYPE=processed_transcript;GMAF=G:0.0005;AFR_MAF=G:0.0020;HGVSC=hgvsc_value;HGVSP=hgvsp_value;polyphen=possibly_damaging(0.859);sift=tolerated(0.07);\n";

    public static final String vepOutputContentWithExtraFields = "" +
            "20_63351_A/G\t20:63351\tG\tENSG00000178591\tENST00000608838\tTranscript\tupstream_gene_variant\t-\t-\t-\t-\t-\trs181305519\tDISTANCE=4540;STRAND=1;SYMBOL=DEFB125;SYMBOL_SOURCE=HGNC;HGNC_ID=18105;BIOTYPE=processed_transcript;GMAF=G:0.0005;AFR_MAF=G:0.0020;polyphen=possibly_damaging(0.859);sift=tolerated(0.07);\n" +
            "20_63360_C/T\t20:63360\tT\tENSG00000178591\tENST00000382410\tTranscript\tupstream_gene_variant\t-\t-\t-\t-\t-\trs186156309\tDISTANCE=4991;STRAND=1;SYMBOL=DEFB125;SYMBOL_SOURCE=HGNC;HGNC_ID=18105;BIOTYPE=protein_coding;CANONICAL=YES;CCDS=CCDS12989.2;ENSP=ENSP00000371847;SWISSPROT=DB125_HUMAN;TREMBL=B2R4E8_HUMAN;UNIPARC=UPI00001A36DE;GMAF=T:0.0014;AMR_MAF=T:0.01;polyphen=possibly_damaging(0.1);sift=tolerated(0.1);\n" +
            "20_63360_C/T\t20:63360\tT\tENSG00000178591\tENST00000608838\tTranscript\tupstream_gene_variant\t-\t-\t-\t-\t-\trs186156309\tDISTANCE=4531;STRAND=1;SYMBOL=DEFB125;SYMBOL_SOURCE=HGNC;HGNC_ID=18105;BIOTYPE=processed_transcript;GMAF=T:0.0014;AMR_MAF=T:0.01;polyphen=possibly_damaging(0.2);sift=tolerated(0.2);\n" +
            "20_63399_G/A\t20:63399\tA\tENSG00000178591\tENST00000382410\tTranscript\tupstream_gene_variant\t-\t-\t-\t-\t-\t-\tDISTANCE=4952;STRAND=1;SYMBOL=DEFB125;SYMBOL_SOURCE=HGNC;HGNC_ID=18105;BIOTYPE=protein_coding;CANONICAL=YES;CCDS=CCDS12989.2;ENSP=ENSP00000371847;SWISSPROT=DB125_HUMAN;TREMBL=B2R4E8_HUMAN;UNIPARC=UPI00001A36DE;polyphen=possibly_damaging(0.859);sift=tolerated(0.07);\n" +
            "20_63399_G/A\t20:63399\tA\tENSG00000178591\tENST00000608838\tTranscript\tupstream_gene_variant\t-\t-\t-\t-\t-\t-\tDISTANCE=4492;STRAND=1;SYMBOL=DEFB125;SYMBOL_SOURCE=HGNC;HGNC_ID=18105;BIOTYPE=processed_transcript;polyphen=possibly_damaging(0.859);sift=tolerated(0.07);\n" +
            "20_63426_G/T\t20:63426\tT\tENSG00000178591\tENST00000382410\tTranscript\tupstream_gene_variant\t-\t-\t-\t-\t-\trs147063585\tDISTANCE=4925;STRAND=1;SYMBOL=DEFB125;SYMBOL_SOURCE=HGNC;HGNC_ID=18105;BIOTYPE=protein_coding;CANONICAL=YES;CCDS=CCDS12989.2;ENSP=ENSP00000371847;SWISSPROT=DB125_HUMAN;TREMBL=B2R4E8_HUMAN;UNIPARC=UPI00001A36DE;GMAF=T:0.0028;AFR_MAF=T:0.01;polyphen=possibly_damaging(0.3);sift=tolerated(0.3);\n" +
            "20_63426_G/T\t20:63426\tT\tENSG00000178591\tENST00000608838\tTranscript\tupstream_gene_variant\t-\t-\t-\t-\t-\trs147063585\tDISTANCE=4465;STRAND=1;SYMBOL=DEFB125;SYMBOL_SOURCE=HGNC;HGNC_ID=18105;BIOTYPE=processed_transcript;GMAF=T:0.0028;AFR_MAF=T:0.01;polyphen=possibly_damaging(0.4);sift=tolerated(0.4);\n";

}
