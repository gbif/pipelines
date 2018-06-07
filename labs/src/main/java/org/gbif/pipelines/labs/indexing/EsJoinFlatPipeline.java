package org.gbif.pipelines.labs.indexing;

import org.gbif.pipelines.config.DataPipelineOptionsFactory;
import org.gbif.pipelines.config.EsProcessingPipelineOptions;
import org.gbif.pipelines.io.avro.InterpretedExtendedRecord;
import org.gbif.pipelines.io.avro.location.LocationRecord;
import org.gbif.pipelines.io.avro.multimedia.MultimediaRecord;
import org.gbif.pipelines.io.avro.taxon.TaxonRecord;
import org.gbif.pipelines.io.avro.temporal.TemporalRecord;

import org.apache.avro.specific.SpecificRecordBase;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.AvroIO;
import org.apache.beam.sdk.io.elasticsearch.ElasticsearchIO;
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

/**
 * PLEASE READ DOCS/ES-JOIN-INDEXING.MD FILE
 *
 * This pipeline reads from interpreted avro records from different categories(location,temporal,common,taxonomy and common) and index them on an Elastic search index.
 * <p>
 * This pipeline is demonstration of using partial updates of Elastic Search to join the different categories of data to one index. It is done based on id key which is common in all categories.
 * <p>
 * Each category generates a flat structure of json which is written in ES in parallel.
 */
public class EsJoinFlatPipeline {

  private static final String ID_KEY = "id";

  public static void main(String[] args) {

    EsProcessingPipelineOptions options = DataPipelineOptionsFactory.createForEs(args);
    Pipeline pipeline = Pipeline.create(options);

    String defTargetDir = options.getDefaultTargetDirectory().endsWith(Path.SEPARATOR)
      ? options.getDefaultTargetDirectory()
      : options.getDefaultTargetDirectory().concat(Path.SEPARATOR);
    String inputDirectory =
      defTargetDir + options.getDatasetId() + Path.SEPARATOR + options.getAttempt() + Path.SEPARATOR;
    String index = options.getESIndexPrefix() + "_" + options.getDatasetId() + "_" + options.getAttempt();

    ElasticsearchIO.ConnectionConfiguration connectionConfiguration =
      ElasticsearchIO.ConnectionConfiguration.create(options.getESAddresses(), index, index);

    final PCollection<String> apply1 =
      pipeline.apply(AvroIO.read(LocationRecord.class).from(inputDirectory + "location/interpreted*.avro"))
        .apply(MapElements.into(TypeDescriptor.of(String.class)).via(SpecificRecordBase::toString))
        .apply(ParDo.of(new AddIdToJson()));

    final PCollection<String> apply2 =
      pipeline.apply(AvroIO.read(TemporalRecord.class).from(inputDirectory + "temporal/interpreted*.avro"))
        .apply(MapElements.into(TypeDescriptor.of(String.class)).via(SpecificRecordBase::toString));

    final PCollection<String> apply3 =
      pipeline.apply(AvroIO.read(MultimediaRecord.class).from(inputDirectory + "multimedia/interpreted*.avro"))
        .apply(MapElements.into(TypeDescriptor.of(String.class)).via(SpecificRecordBase::toString));

    final PCollection<String> apply4 =
      pipeline.apply(AvroIO.read(TaxonRecord.class).from(inputDirectory + "taxonomy/interpreted*.avro"))
        .apply(MapElements.into(TypeDescriptor.of(String.class)).via(SpecificRecordBase::toString));

    final PCollection<String> apply5 =
      pipeline.apply(AvroIO.read(InterpretedExtendedRecord.class).from(inputDirectory + "common/interpreted*.avro"))
        .apply(MapElements.into(TypeDescriptor.of(String.class)).via(SpecificRecordBase::toString));

    PCollectionList.of(apply1)
      .and(apply2)
      .and(apply3)
      .and(apply4)
      .and(apply5)
      .apply(Flatten.pCollections())
      .apply(ElasticsearchIO.write()
               .withConnectionConfiguration(connectionConfiguration)
               .withIdFn(node -> node.get(ID_KEY).textValue())
               .withUsePartialUpdate(true)
               .withMaxBatchSize(options.getESMaxBatchSize()));

    pipeline.run().waitUntilFinish();

  }

  /**
   * DoFn adds a field 'id' in json of location category
   */
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
