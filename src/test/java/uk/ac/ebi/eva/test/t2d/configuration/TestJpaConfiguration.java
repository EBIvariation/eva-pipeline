package uk.ac.ebi.eva.test.t2d.configuration;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import uk.ac.ebi.eva.t2d.repository.CommonSampleRepository;
import uk.ac.ebi.eva.t2d.repository.DatasetIdToPhenotypeRepository;
import uk.ac.ebi.eva.t2d.repository.DatasetMetadataRepository;
import uk.ac.ebi.eva.t2d.repository.DatasetPhenotypeToTableRepository;
import uk.ac.ebi.eva.t2d.repository.DatasetVersionMetadataRepository;
import uk.ac.ebi.eva.t2d.repository.PhenotypeRepository;
import uk.ac.ebi.eva.t2d.repository.PropertyRepository;
import uk.ac.ebi.eva.t2d.repository.PropertyToDatasetAndPhenotypeRepository;
import uk.ac.ebi.eva.t2d.repository.PropertyToDatasetRepository;
import uk.ac.ebi.eva.t2d.repository.SamplePropertyRepository;
import uk.ac.ebi.eva.t2d.repository.SamplePropertyToDatasetRepository;
import uk.ac.ebi.eva.t2d.repository.SamplesDatasetMetadataRepository;
import uk.ac.ebi.eva.t2d.repository.VariantInfoRepository;
import uk.ac.ebi.eva.t2d.services.T2dJpaService;
import uk.ac.ebi.eva.t2d.services.T2dService;


@Configuration
public class TestJpaConfiguration {

    @Bean
    public T2dService t2dService(DatasetMetadataRepository datasetMetadataRepository,
                                 DatasetVersionMetadataRepository datasetVersionMetadataRepository,
                                 DatasetPhenotypeToTableRepository datasetPhenotypeToTableRepository,
                                 DatasetIdToPhenotypeRepository datasetIdToPhenotypeRepository,
                                 PropertyRepository propertyRepository,
                                 PropertyToDatasetRepository propertyToDatasetRepository,
                                 PhenotypeRepository phenotypeRepository,
                                 PropertyToDatasetAndPhenotypeRepository propertyToDatasetAndPhenotypeRepository,
                                 CommonSampleRepository commonSampleRepository,
                                 SamplesDatasetMetadataRepository samplesDatasetMetadataRepository,
                                 SamplePropertyRepository samplePropertyRepository,
                                 SamplePropertyToDatasetRepository samplePropertyToDatasetRepository,
                                 VariantInfoRepository variantInfoRepository) {
        return new T2dJpaService(datasetMetadataRepository,
                datasetVersionMetadataRepository,
                datasetPhenotypeToTableRepository,
                datasetIdToPhenotypeRepository,
                propertyRepository,
                propertyToDatasetRepository,
                phenotypeRepository,
                propertyToDatasetAndPhenotypeRepository,
                commonSampleRepository,
                samplesDatasetMetadataRepository,
                samplePropertyRepository,
                samplePropertyToDatasetRepository,
                variantInfoRepository);
    }

}
