# Transform example

Examples of using project API and base classes

1) [Avro](https://avro.apache.org/docs/current/) schema - [example-record.avsc](./src/main/resources/example-record.avsc) is used to generate target data class.
2) Interpretation [ExampleInterpreter.java](./src/main/java/org/gbif/pipelines/examples/ExampleInterpreter.java) class uses source data object to apply some logic and sets data to the target object.
3) [ExampleTransform.java](./src/main/java/org/gbif/pipelines/examples/ExampleTransform.java) is [Apache Beam](https://beam.apache.org/get-started/beam-overview/) ParDo tranformation, uses [ExampleInterpreter.java](./src/main/java/org/gbif/pipelines/examples/ExampleInterpreter.java) and [Interpretation.java](../sdks/core/src/main/java/org/gbif/pipelines/core/Interpretation.java).
4) [ExamplePipeline.java](./src/main/java/org/gbif/pipelines/examples/ExamplePipeline.java) is [Apache Beam](https://beam.apache.org/get-started/beam-overview/) pipeline uses [ExampleTransform.java](./src/main/java/org/gbif/pipelines/examples/ExampleTransform.java) as a ParDo transformation, also you can find example of a [Darwin Core Archive](https://www.tdwg.org/standards/dwc/) - [example.zip](./src/main/resources/example.zip) and example of pipeline options - [example.properties](./src/main/resources/example.properties) to run the pipeline.blob/master/examples/src/main/java/or