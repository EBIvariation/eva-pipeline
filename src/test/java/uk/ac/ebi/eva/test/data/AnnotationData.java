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
package uk.ac.ebi.eva.test.data;

public class AnnotationData {
    public final static String VARIANT_ANNOTATION_JSON = "{  \n" +
            "   \"ct\":[  \n" +
            "      {  \n" +
            "         \"gn\":\"geneName\",\n" +
            "         \"ensg\":\"ensemblGeneId\",\n" +
            "         \"enst\":\"ensemblTranscriptId\",\n" +
            "         \"codon\":\"codon\",\n" +
            "         \"strand\":\"strand\",\n" +
            "         \"bt\":\"biotype\",\n" +
            "         \"aaChange\":\"aaChange\",\n" +
            "         \"so\":[  \n" +
            "            1893,\n" +
            "            1575\n" +
            "         ],\n" +
            "         \"polyphen\":{  \n" +
            "            \"sc\":1.0,\n" +
            "            \"desc\":\"Polyphen description\"\n" +
            "         },\n" +
            "         \"sift\":{  \n" +
            "            \"sc\":1.0,\n" +
            "            \"desc\":\"Sift description\"\n" +
            "         }\n" +
            "      }\n" +
            "   ],\n" +
            "   \"xrefs\":[  \n" +
            "      {  \n" +
            "         \"id\":\"OS01G0112100\",\n" +
            "         \"src\":\"ensemblGene\"\n" +
            "      },\n" +
            "      {  \n" +
            "         \"id\":\"ensemblGeneId\",\n" +
            "         \"src\":\"ensemblGene\"\n" +
            "      },\n" +
            "      {  \n" +
            "         \"id\":\"geneName\",\n" +
            "         \"src\":\"HGNC\"\n" +
            "      },\n" +
            "      {  \n" +
            "         \"id\":\"ensemblTranscriptId\",\n" +
            "         \"src\":\"ensemblTranscript\"\n" +
            "      }\n" +
            "   ]\n" +
            "}";
}
