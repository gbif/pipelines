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
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.values.TypeDescriptor;

public class AvroToESJoinPipeline {

  private static final String INPUT =
    "hdfs://ha-nn/user/hive/warehouse/gbif-data/00ddc92e-fde1-4dda-8712-ff46582a5e6b/1/";
  private static final String[] ES_ADDRESSES =
    {"http://c3n1.gbif.org:9200", "http://c3n2.gbif.org:9200", "http://c3n3.gbif.org:9200"};
  private static final String ES_IDX = "interpreted-dataset";

  public static void main(String[] args) {

    DataProcessingPipelineOptions options = DataPipelineOptionsFactory.create(args);
    Pipeline pipeline = Pipeline.create(options);

    pipeline.apply(AvroIO.read(Location.class).from(INPUT + "location/interpreted*.avro"))
      .apply(MapElements.into(TypeDescriptor.of(String.class)).via((record) -> record.toString()))
      .apply(PatchedElasticsearchIO.write()
               .withConnectionConfiguration(PatchedElasticsearchIO.ConnectionConfiguration.create(ES_ADDRESSES,
                                                                                                  ES_IDX,
                                                                                                  ES_IDX))
               .withIdFn((node) -> node.get("occurrenceID").textValue())
               .withUsePartialUpdate(true));

    pipeline.apply(AvroIO.read(TemporalRecord.class).from(INPUT + "temporal/interpreted*.avro"))
      .apply(MapElements.into(TypeDescriptor.of(String.class)).via((record) -> record.toString()))
      .apply(PatchedElasticsearchIO.write()
               .withConnectionConfiguration(PatchedElasticsearchIO.ConnectionConfiguration.create(ES_ADDRESSES,
                                                                                                  ES_IDX,
                                                                                                  ES_IDX))
               .withIdFn((node) -> node.get("id").textValue())
               .withUsePartialUpdate(true));

    pipeline.apply(AvroIO.read(MultimediaRecord.class).from(INPUT + "multimedia/interpreted*.avro"))
      .apply(MapElements.into(TypeDescriptor.of(String.class)).via((record) -> record.toString()))
      .apply(PatchedElasticsearchIO.write()
               .withConnectionConfiguration(PatchedElasticsearchIO.ConnectionConfiguration.create(ES_ADDRESSES,
                                                                                                  ES_IDX,
                                                                                                  ES_IDX))
               .withIdFn((node) -> node.get("id").textValue())
               .withUsePartialUpdate(true));

    pipeline.apply(AvroIO.read(TaxonRecord.class).from(INPUT + "taxonomy/interpreted*.avro"))
      .apply(MapElements.into(TypeDescriptor.of(String.class)).via((record) -> record.toString()))
      .apply(PatchedElasticsearchIO.write()
               .withConnectionConfiguration(PatchedElasticsearchIO.ConnectionConfiguration.create(ES_ADDRESSES,
                                                                                                  ES_IDX,
                                                                                                  ES_IDX))
               .withIdFn((node) -> node.get("id").textValue())
               .withUsePartialUpdate(true));

    pipeline.apply(AvroIO.read(InterpretedExtendedRecord.class).from(INPUT + "common/interpreted*.avro"))
      .apply(MapElements.into(TypeDescriptor.of(String.class)).via((record) -> record.toString()))
      .apply(PatchedElasticsearchIO.write()
               .withConnectionConfiguration(PatchedElasticsearchIO.ConnectionConfiguration.create(ES_ADDRESSES,
                                                                                                  ES_IDX,
                                                                                                  ES_IDX))
               .withIdFn((node) -> node.get("id").textValue())
               .withUsePartialUpdate(true));

    pipeline.run().waitUntilFinish();

  }

}
