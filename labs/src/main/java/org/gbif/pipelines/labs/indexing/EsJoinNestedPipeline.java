package org.gbif.pipelines.labs.indexing;

import org.gbif.pipelines.config.DataPipelineOptionsFactory;
import org.gbif.pipelines.config.EsProcessingPipelineOptions;
import org.gbif.pipelines.io.avro.InterpretedExtendedRecord;
import org.gbif.pipelines.io.avro.location.LocationRecord;
import org.gbif.pipelines.io.avro.multimedia.MultimediaRecord;
import org.gbif.pipelines.io.avro.taxon.TaxonRecord;
import org.gbif.pipelines.io.avro.temporal.TemporalRecord;

import java.io.IOException;
import java.util.Optional;

import org.apache.avro.specific.SpecificRecordBase;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.AvroIO;
import org.apache.beam.sdk.io.elasticsearch.ElasticsearchIO;
import org.apache.beam.sdk.transforms.Flatten;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.hadoop.fs.Path;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.node.ObjectNode;
/**
 * PLEASE READ DOCS/ES-JOIN-INDEXING.MD FILE
 *
 * This pipeline reads from interpreted avro records from different categories(location,temporal,common,taxonomy and common) and index them on an Elastic search index.
 * <p>
 * This pipeline is demonstration of using partial updates of Elastic Search to join the different categories of data to one index. It is done based on id key which is common in all categories.
 *
 * Each category generates a nested structure of json which is written in ES in parallel.
 */
public class EsJoinNestedPipeline {

  private static final String ID_KEY = "id";
  private static final String OCCURRENCE_ID_KEY = "occurrenceID";
  private static final String TEMPORAL = "temporal-record";
  private static final String LOCATION = "location-record";
  private static final String TAXON = "taxon-record";
  private static final String MULTIMEDIA = "multimedia-record";
  private static final String COMMON = "common-record";

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
    PCollection<String> apply1 = pipeline.apply("Reading location data", AvroIO.read(LocationRecord.class).from(inputDirectory + "location/interpreted*.avro"))
      .apply("Converting location data to json", MapElements.into(TypeDescriptor.of(String.class)).via(new MapFn(LOCATION, OCCURRENCE_ID_KEY)));

    PCollection<String> apply2 = pipeline.apply("Reading temporal data", AvroIO.read(TemporalRecord.class).from(inputDirectory + "temporal/interpreted*.avro"))
      .apply("Converting temporal data to json", MapElements.into(TypeDescriptor.of(String.class)).via(new MapFn(TEMPORAL, ID_KEY)));

    PCollection<String> apply3 = pipeline.apply("Reading Multimedia data",AvroIO.read(MultimediaRecord.class).from(inputDirectory + "multimedia/interpreted*.avro"))
      .apply("Converting Multimedia data to json", MapElements.into(TypeDescriptor.of(String.class)).via(new MapFn(MULTIMEDIA, ID_KEY)));

    PCollection<String> apply4 = pipeline.apply("Reading Taxon data", AvroIO.read(TaxonRecord.class).from(inputDirectory + "taxonomy/interpreted*.avro"))
      .apply("Converting Taxon data to json", MapElements.into(TypeDescriptor.of(String.class)).via(new MapFn(TAXON, ID_KEY)));

    PCollection<String> apply5 = pipeline.apply("Reading Common data", AvroIO.read(InterpretedExtendedRecord.class).from(inputDirectory + "common/interpreted*.avro"))
      .apply("Converting Common data to json", MapElements.into(TypeDescriptor.of(String.class)).via(new MapFn(COMMON, ID_KEY)));

    PCollectionList.of(apply1).and(apply2).and(apply3).and(apply4).and(apply5).apply(Flatten.pCollections()).apply(ElasticsearchIO.write().withConnectionConfiguration(connectionConfiguration)
                                                                                                                     .withIdFn(node -> node.get(ID_KEY).textValue())
                                                                                                                     .withUsePartialUpdate(true)
                                                                                                                     .withMaxBatchSize(options.getESMaxBatchSize()));

    pipeline.run().waitUntilFinish();

  }

  /**
   * Map function creating nested json objects of different categories based on their type
   */
  static class MapFn implements SerializableFunction<SpecificRecordBase, String> {

    private final String type;
    private final String idField;

    MapFn(String type, String idField) {
      this.type = type;
      this.idField = idField;
    }

    @Override
    public String apply(SpecificRecordBase input) {
      ObjectMapper mapper = new ObjectMapper();
      Optional<JsonNode> inputNode;
      try {
        inputNode = Optional.of(mapper.readTree(input.toString()));
      } catch (IOException e) {
        inputNode = Optional.empty();
      }
      ObjectNode objectNode = mapper.createObjectNode();
      inputNode.ifPresent(record -> {
        objectNode.put(ID_KEY, record.get(idField));
        objectNode.put(type, record);
      });

      return objectNode.toString();
    }
  }

}
