package org.gbif.pipelines.labs;

import org.gbif.pipelines.config.DataPipelineOptionsFactory;
import org.gbif.pipelines.config.DataProcessingPipelineOptions;
import org.gbif.pipelines.io.avro.InterpretedExtendedRecord;
import org.gbif.pipelines.io.avro.Location;
import org.gbif.pipelines.io.avro.MultimediaRecord;
import org.gbif.pipelines.io.avro.TaxonRecord;
import org.gbif.pipelines.io.avro.TemporalRecord;
import org.gbif.pipelines.labs.io.PatchedElasticsearchIO;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.AvroIO;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.Flatten;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.hadoop.fs.Path;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.node.ObjectNode;

public class AvroToESJoinPipeline {

  private static final String ID_KEY = "id";

  public static void main(String[] args) {

    DataProcessingPipelineOptions options = DataPipelineOptionsFactory.create(args);
    Pipeline pipeline = Pipeline.create(options);

    String defTargetDir = options.getDefaultTargetDirectory().endsWith(Path.SEPARATOR)
      ? options.getDefaultTargetDirectory()
      : options.getDefaultTargetDirectory().concat(Path.SEPARATOR);
    String inputDirectory =
      defTargetDir + options.getDatasetId() + Path.SEPARATOR + options.getAttempt() + Path.SEPARATOR;
    String index = options.getESIndexPrefix() + "_" + options.getDatasetId() + "_" + options.getAttempt();

    PatchedElasticsearchIO.ConnectionConfiguration connectionConfiguration =
      PatchedElasticsearchIO.ConnectionConfiguration.create(options.getESAddresses(), index, index);

    final PCollection<String> apply1 =
      pipeline.apply(AvroIO.read(Location.class).from(inputDirectory + "location/interpreted*.avro"))
        .apply(MapElements.into(TypeDescriptor.of(String.class)).via((record) -> record.toString()))
        .apply(ParDo.of(new AddIdToJson()));

    final PCollection<String> apply2 =
      pipeline.apply(AvroIO.read(TemporalRecord.class).from(inputDirectory + "temporal/interpreted*.avro"))
        .apply(MapElements.into(TypeDescriptor.of(String.class)).via((record) -> record.toString()));

    final PCollection<String> apply3 =
      pipeline.apply(AvroIO.read(MultimediaRecord.class).from(inputDirectory + "multimedia/interpreted*.avro"))
        .apply(MapElements.into(TypeDescriptor.of(String.class)).via((record) -> record.toString()));

    final PCollection<String> apply4 =
      pipeline.apply(AvroIO.read(TaxonRecord.class).from(inputDirectory + "taxonomy/interpreted*.avro"))
        .apply(MapElements.into(TypeDescriptor.of(String.class)).via((record) -> record.toString()));

    final PCollection<String> apply5 =
      pipeline.apply(AvroIO.read(InterpretedExtendedRecord.class).from(inputDirectory + "common/interpreted*.avro"))
        .apply(MapElements.into(TypeDescriptor.of(String.class)).via((record) -> record.toString()));

    PCollectionList.of(apply1)
      .and(apply2)
      .and(apply3)
      .and(apply4)
      .and(apply5)
      .apply(Flatten.pCollections())
      .apply(PatchedElasticsearchIO.write()
               .withConnectionConfiguration(connectionConfiguration)
               .withIdFn((node) -> node.get(ID_KEY).textValue())
               .withUsePartialUpdate(true)
               .withMaxBatchSize(options.getESBatchSize()));

    pipeline.run().waitUntilFinish();

  }

  static class AddIdToJson extends DoFn<String, String> {

    private static final String OCCURRENCEID_KEY = "occurrenceID";
    private static final String ID_KEY = "id";

    @ProcessElement
    public void processElement(ProcessContext processContext) {

      String text = processContext.element();
      try {
        ObjectNode obj = (ObjectNode) new ObjectMapper().readTree(text);
        if (obj.get(OCCURRENCEID_KEY) != null) obj.put(ID_KEY, obj.get(OCCURRENCEID_KEY));
        processContext.output(obj.toString());
      } catch (Exception ex) {
        processContext.output(text);
      }
    }
  }

}
