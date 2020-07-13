source set-env.sh

echo $(date)
SECONDS=0
/data/spark/bin/spark-submit \
--name "interpret $1" \
--conf spark.default.parallelism=144 \
--num-executors 16 \
--executor-cores 8 \
--executor-memory 7G \
--driver-memory 1G \
--class au.org.ala.pipelines.beam.ALAVerbatimToInterpretedPipeline \
--master spark://aws-spark-quoll-1.ala:7077 \
--driver-java-options "-Dlog4j.configuration=file:/efs-mount-point/log4j.properties" \
/efs-mount-point/pipelines.jar \
--appName="Interpretation for $1" \
--datasetId=$1 \
--attempt=1 \
--interpretationTypes=ALL \
--runner=SparkRunner \
--targetPath=hdfs://aws-spark-quoll-1.ala:9000/pipelines-data \
--inputPath=hdfs://aws-spark-quoll-1.ala:9000/pipelines-data/$1/1/verbatim.avro \
--metaFileName=interpretation-metrics.yml \
--properties=hdfs://aws-spark-quoll-1.ala:9000/pipelines.properties \
--coreSiteConfig=$HDFS_CONF \
--hdfsSiteConfig=$HDFS_CONF \
--useExtendedRecordId=true \
--skipRegisrtyCalls=true
echo $(date)
duration=$SECONDS
echo "INTERPRET $1 took $(($duration / 60)) minutes and $(($duration % 60)) seconds."

echo $(date)
SECONDS=0
/data/spark/bin/spark-submit \
--name "Export LatLng $1" \
--num-executors 8 \
--executor-cores 8 \
--executor-memory 16G \
--driver-memory 4G \
--class au.org.ala.pipelines.beam.ALAInterpretedToLatLongCSVPipeline  \
--master spark://aws-spark-quoll-1.ala:7077 \
--driver-java-options "-Dlog4j.configuration=file:/efs-mount-point/log4j.properties" \
/efs-mount-point/pipelines.jar \
--appName="Lat Long export for $1" \
--datasetId=$1 \
--attempt=1 \
--runner=SparkRunner \
--properties=hdfs://aws-spark-quoll-1.ala:9000/pipelines.properties \
--coreSiteConfig=$HDFS_CONF \
--hdfsSiteConfig=$HDFS_CONF \
--inputPath=hdfs://aws-spark-quoll-1.ala:9000/pipelines-data \
--targetPath=hdfs://aws-spark-quoll-1.ala:9000/pipelines-data 
echo $(date)
duration=$SECONDS
echo "LATLNG $1 took $(($duration / 60)) minutes and $(($duration % 60)) seconds."

echo $(date)
SECONDS=0
java -Xmx8g -Xmx8g -XX:+UseG1GC -cp /efs-mount-point/pipelines.jar au.org.ala.sampling.LayerCrawler \
 --appName="Sample for $1" \
 --datasetId=$1 \
 --attempt=1 \
 --runner=SparkRunner \
--properties=hdfs://aws-spark-quoll-1.ala:9000/pipelines.properties \
--coreSiteConfig=$HDFS_CONF \
--hdfsSiteConfig=$HDFS_CONF \
--inputPath=hdfs://aws-spark-quoll-1.ala:9000/pipelines-data \
--targetPath=hdfs://aws-spark-quoll-1.ala:9000/pipelines-data \
--metaFileName=indexing-metrics.yml 
echo $(date)
duration=$SECONDS
echo "SAMPLE $1 took $(($duration / 60)) minutes and $(($duration % 60)) seconds."

echo $(date)
SECONDS=0
 /data/spark/bin/spark-submit \
--name "add-sampling $1" \
--conf spark.default.parallelism=192 \
--num-executors 24 \
--executor-cores 8 \
--executor-memory 7G \
--driver-memory 1G \
--class au.org.ala.pipelines.beam.ALASamplingToAvroPipeline \
--master spark://aws-spark-quoll-1.ala:7077 \
--driver-java-options "-Dlog4j.configuration=file:/efs-mount-point/log4j.properties" \
/efs-mount-point/pipelines.jar \
--appName="Add Sampling for $$1" \
--datasetId=$1 \
--attempt=1 \
--interpretationTypes=ALL \
--runner=SparkRunner \
--metaFileName=interpretation-metrics.yml \
--properties=hdfs://aws-spark-quoll-1.ala:9000/pipelines.properties \
--coreSiteConfig=$HDFS_CONF \
--hdfsSiteConfig=$HDFS_CONF \
--inputPath=hdfs://aws-spark-quoll-1.ala:9000/pipelines-data \
--targetPath=hdfs://aws-spark-quoll-1.ala:9000/pipelines-data
echo $(date)
duration=$SECONDS
echo "SAMPLE-AVRO to $1 took $(($duration / 60)) minutes and $(($duration % 60)) seconds."

echo $(date)
SECONDS=0
/data/spark/bin/spark-submit \
--name "uuid-minting $1" \
--num-executors 24 \
--executor-cores 8 \
--executor-memory 7G \
--driver-memory 1G \
--class au.org.ala.pipelines.beam.ALAUUIDMintingPipeline \
--master spark://aws-spark-quoll-1.ala:7077 \
--driver-java-options "-Dlog4j.configuration=file:/efs-mount-point/log4j.properties" \
/efs-mount-point/pipelines.jar \
--appName="UUID minting for $1" \
--datasetId=$1 \
--attempt=1 \
--interpretationTypes=ALL \
--runner=SparkRunner \
--properties=hdfs://aws-spark-quoll-1.ala:9000/pipelines.properties \
--coreSiteConfig=$HDFS_CONF \
--hdfsSiteConfig=$HDFS_CONF \
--inputPath=hdfs://aws-spark-quoll-1.ala:9000/pipelines-data \
--targetPath=hdfs://aws-spark-quoll-1.ala:9000/pipelines-data
echo $(date)
duration=$SECONDS
echo "UUIDS to $1 took $(($duration / 60)) minutes and $(($duration % 60)) seconds."

echo $(date)
SECONDS=0
/data/spark/bin/spark-submit \
--conf spark.default.parallelism=192 \
--num-executors 24 \
--executor-cores 8 \
--executor-memory 7G \
--driver-memory 1G \
--class au.org.ala.pipelines.beam.ALAInterpretedToSolrIndexPipeline  \
--master spark://aws-spark-quoll-1.ala:7077 \
--driver-java-options "-Dlog4j.configuration=file:/efs-mount-point/log4j.properties" \
/efs-mount-point/pipelines.jar \
--appName="SOLR indexing for $1" \
--datasetId=$1 \
--attempt=1 \
--runner=SparkRunner \
--properties=hdfs://aws-spark-quoll-1.ala:9000/pipelines.properties \
--coreSiteConfig=$HDFS_CONF \
--hdfsSiteConfig=$HDFS_CONF \
--inputPath=hdfs://aws-spark-quoll-1.ala:9000/pipelines-data \
--targetPath=hdfs://aws-spark-quoll-1.ala:9000/pipelines-data \
--metaFileName=indexing-metrics.yml \
--includeSampling=true \
--zkHost=aws-zoo-quoll-1.ala:2181,aws-zoo-quoll-2.ala:2181,aws-zoo-quoll-3.ala:2181,aws-zoo-quoll-4.ala:2181,aws-zoo-quoll-5.ala:2181 \
--solrCollection=biocache
echo $(date)
duration=$SECONDS
echo "SOLR to $1 took $(($duration / 60)) minutes and $(($duration % 60)) seconds."