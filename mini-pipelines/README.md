# Mini pipelines #
This project aims to provide a set of mini pipelines that can be used for testing or simply as tools for developers or small organizations. 

The jar generated to be used by others is `mini-pipelines-{version}.jar`. It can be downloaded from http://repository.gbif.org/content/groups/gbif/org/gbif/pipelines/mini-pipelines/.

## Dwca mini pipeline ##
This mini pipeline was created to work with Dwc-A as the input of the pipeline.

As defined in [this issue](https://github.com/gbif/pipelines/issues/116) this pipeline has the following limitations:
- It is intended to be used with small datasets (less than 1 million records).
- It works only in the local filesystem.
- All the interpretation based on external service will use the GBIF services only.
- The Elasticsearch schema will be aligned to the GBIF schema but it can change over time, users of this tool should expect continuos changes to the ES schema that could impact on the services depending on it.

### How to run the pipeline ###
The main class that runs this pipeline is `DwcaPipeline` and it uses a `DwcaMiniPipelineOptions` for configuration.

The parameters that can be used can be seen using the `--help=DwcaMiniPipelineOptions` option:

~~~~
java -jar mini-pipelines.jar --help=DwcaMiniPipelineOptions
~~~~ 

This is an example to run this pipeline with the minimum required parameters:

~~~~
java -jar mini-pipelines.jar --inputPath=dwca.zip --targetPath=output --datasetId=abcde12345 --attempt=1 --gbifEnv=PROD --ESHosts=http://localhost:9200
~~~~ 

The output is the records indexed in ES. 
By default, the ES index name follows the format `{datasetId}_{attempt}` - in this example it's `abcde12345_1`. This index is added to the alias specified in the `ESAlias` parameter. By default it's `occurrence`.

If we want the intermediate outputs to be written to avro files we need to set the `ignoreIntermediateOutputs`to `false`:

~~~~
java -jar mini-pipelines.jar --inputPath=dwca.zip --targetPath=output --datasetId=abcd1234 --attempt=1 --gbifEnv=PROD --ESHosts=http://localhost:9200 --ignoreIntermediateOutputs=false
~~~~ 

This generates an output like this:

 <img src="docs/output_generated.png">


Other examples of commands:
- Only DWCA_TO_AVRO step: 
~~~~
java -jar mini-pipelines.jar --inputPath=dwca.zip --targetPath=output --datasetId=abcd1234 --attempt=1 --gbifEnv=PROD --pipelineStep=DWCA_TO_AVRO
~~~~ 

- INTERPRET step: 
~~~~
java -jar mini-pipelines.jar --inputPath=dwca.zip --targetPath=output --datasetId=abcd1234 --attempt=1 --gbifEnv=PROD --pipelineStep=INTERPRET
~~~~ 

NOTE: at the time being the ES schema is temporary and the development of the pipeline is still in an early stage, therefore issues may be encountered.

#### Spark runner ####
The Spark dependencies are included in the project so the pipelines can be run in an embedded Spark in our localhost. It is recommended to use this runner 
because the default runner (DirectRunner) is very limited and can handle only some thousands of records and the performance is very poor.
  
To run the pipeline with Spark, we need to use the SparkRunner. It's also recommended to increase the memory size and use the G1 garbage collector:

~~~~
java -XX:+UseG1GC -Xms256M -Xmx8G -jar mini-pipelines.jar --inputPath=dwca.zip --targetPath=output --datasetId=abcde12345 --attempt=1 --gbifEnv=UAT --ESHosts=http://localhost:9200 --runner=SparkRunner
~~~~

By default, Spark uses 4 threads, but we can customize it by using the `sparkMaster` option:

~~~~
java -XX:+UseG1GC -Xms256M -Xmx8G -jar mini-pipelines.jar --inputPath=dwca.zip --targetPath=output --datasetId=abcde12345 --attempt=1 --gbifEnv=UAT --ESHosts=http://localhost:9200 --runner=SparkRunner --sparkMaster=local[8]
~~~~