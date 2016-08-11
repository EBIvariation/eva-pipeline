java -jar eva-pipeline/target/eva-pipeline-0.1.jar \
 --spring.batch.job.names=variantAnnotJob \
 --logging.level.embl.ebi.variation.eva=DEBUG \
 --logging.level.org.opencb.opencga=DEBUG \
 --logging.level.org.springframework=INFO \
 dbName=eva_testing \
 storageEngine=mongodb \
 opencga.app.home=/homes/cyenyxe/spring-batch-pipeline/opencga-annotation-e82 \
 vepInput=/gpfs/nobackup/eva/cyenyxe/vep-annotation/variants.preannot.tsv.gz \
 vepPath="/nfs/production3/eva/VEP/VEP_releases/82/ensembl-tools-release-82/scripts/variant_effect_predictor/variant_effect_predictor.pl"
 vepParameters="--force_overwrite --cache --cache_version 26 -dir /nfs/production3/eva/VEP/cache_1 --offline -o STDOUT --species anopheles_gambiae --everything" \
 vepFasta="--fasta /nfs/production3/eva/VEP/cache_1/anopheles_gambiae/26_AgamP4/Anopheles_gambiae.AgamP4.26.dna.genome.fa" \
 vepOutput=/gpfs/nobackup/eva/cyenyxe/vep-annotation/variants.annot.tsv.gz

