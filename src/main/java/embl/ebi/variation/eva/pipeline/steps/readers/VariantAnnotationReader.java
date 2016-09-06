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
package embl.ebi.variation.eva.pipeline.steps.readers;

import embl.ebi.variation.eva.pipeline.annotation.GzipLazyResource;
import embl.ebi.variation.eva.pipeline.annotation.load.VariantAnnotationLineMapper;
import org.opencb.biodata.models.variant.annotation.VariantAnnotation;
import org.opencb.datastore.core.ObjectMap;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.core.io.Resource;

/**
 * Reader of VEP output annotation flat file. The file should be zipped.
 *
 * VEP output format:
 *
 *  20_60343_G/A	20:60343	A	-	-	-	intergenic_variant	-	-	-	-	-	-
 *  20_60419_A/G	20:60419	G	-	-	-	intergenic_variant	-	-	-	-	-	-
 *  20_60479_C/T	20:60479	T	-	-	-	intergenic_variant	-	-	-	-	-	rs149529999	GMAF=T:0.0018;AFR_MAF=T:0.01;AMR_MAF=T:0.0028
 *  ...
 *
 */
public class VariantAnnotationReader extends FlatFileItemReader<VariantAnnotation>{

    public VariantAnnotationReader(ObjectMap pipelineOptions) {
        Resource resource = new GzipLazyResource(pipelineOptions.getString("vep.output"));
        setResource(resource);
        setLineMapper(new VariantAnnotationLineMapper());
    }
}
