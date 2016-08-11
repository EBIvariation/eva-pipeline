java -jar eva-pipeline/target/eva-pipeline-0.1.jar \
 --spring.batch.job.names=aggregatedVariantJob \
 input=data/aggregated.vcf \
 fileId=5 \
 aggregated=BASIC \
 studyType=COLLECTION \
 studyName=studyName \
 studyId=7 \
 outputDir= \
 pedigree= \
 dbName=aggregatedbatch \
 samples=sample1,sample2,sample3,sample4 \
 storageEngine=mongodb \
 compressExtension=.gz \
 includeSrc=FIRST_8_COLUMNS \
 skipLoad=false \
 opencga.app.home=/opt/opencga/

