#!/usr/bin/env bash
source set-env.sh
#!/usr/bin/env bash
echo "interpret verbatim.arvo file."

echo $(date)
SECONDS=0
java -Xmx1g -XX:+UseG1GC  -Dspark.master=local[*]  -cp $PIPELINES_JAR au.org.ala.pipelines.java.ALAVerbatimToInterpretedPipeline \
--datasetId=$1 \
--config=../configs/la-pipelines.yaml,../configs/la-pipelines-local.yaml

echo $(date)
duration=$SECONDS
echo "Interpretation of $1 took $(($duration / 60)) minutes and $(($duration % 60)) seconds."