package org.gbif.pipelines.interpretation;

import org.gbif.pipelines.GbifInterpretationType;
import org.gbif.pipelines.config.DataPipelineOptionsFactory;
import org.gbif.pipelines.config.DataProcessingPipelineOptions;
import org.gbif.pipelines.core.ws.config.Config;
import org.gbif.pipelines.core.ws.config.HttpConfigFactory;
import org.gbif.pipelines.core.ws.config.Service;
import org.gbif.pipelines.io.avro.ExtendedRecord;
import org.gbif.pipelines.io.avro.InterpretedExtendedRecord;
import org.gbif.pipelines.io.avro.MetadataRecord;
import org.gbif.pipelines.io.avro.location.LocationRecord;
import org.gbif.pipelines.io.avro.multimedia.MultimediaRecord;
import org.gbif.pipelines.io.avro.taxon.TaxonRecord;
import org.gbif.pipelines.io.avro.temporal.TemporalRecord;
import org.gbif.pipelines.transform.AvroOutputTransform;
import org.gbif.pipelines.transform.record.InterpretedExtendedRecordTransform;
import org.gbif.pipelines.transform.record.LocationRecordTransform;
import org.gbif.pipelines.transform.record.MetadataRecordTransform;
import org.gbif.pipelines.transform.record.MultimediaRecordTransform;
import org.gbif.pipelines.transform.record.TaxonRecordTransform;
import org.gbif.pipelines.transform.record.TemporalRecordTransform;
import org.gbif.pipelines.transform.validator.UniqueOccurrenceIdTransform;
import org.gbif.pipelines.utils.FsUtils;

import java.nio.file.Paths;
import java.util.List;
import java.util.function.Predicate;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.io.AvroIO;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.Values;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.gbif.pipelines.GbifInterpretationType.ALL;
import static org.gbif.pipelines.GbifInterpretationType.COMMON;
import static org.gbif.pipelines.GbifInterpretationType.LOCATION;
import static org.gbif.pipelines.GbifInterpretationType.METADATA;
import static org.gbif.pipelines.GbifInterpretationType.MULTIMEDIA;
import static org.gbif.pipelines.GbifInterpretationType.TAXONOMY;
import static org.gbif.pipelines.GbifInterpretationType.TEMPORAL;
import static org.gbif.pipelines.core.ws.config.Service.DATASET_META;
import static org.gbif.pipelines.core.ws.config.Service.GEO_CODE;
import static org.gbif.pipelines.core.ws.config.Service.SPECIES_MATCH2;

public class InterpretationPipeline {

  private static final Logger LOG = LoggerFactory.getLogger(InterpretationPipeline.class);

  private final DataProcessingPipelineOptions options;

  private InterpretationPipeline(DataProcessingPipelineOptions options) {
    this.options = options;
  }

  public static InterpretationPipeline create(DataProcessingPipelineOptions options) {
    return new InterpretationPipeline(options);
  }

  public static void main(String[] args) {
    DataProcessingPipelineOptions options = DataPipelineOptionsFactory.create(args);
    InterpretationPipeline.create(options).run();
  }

  public PipelineResult.State run() {

    options.setWriteOutput(true);
    String inputFile = options.getInputPath();
    List<String> types = options.getInterpretationTypes();
    Predicate<GbifInterpretationType> checkType = t -> types.contains(ALL.name()) || types.contains(t.name());

    LOG.info("Creating pipeline from options");
    Pipeline p = Pipeline.create(options);

    if (checkType.test(METADATA)) {
      LOG.info("Adding metadata interpretation");
      MetadataRecordTransform metadataTr = MetadataRecordTransform.create(getWsConfig(DATASET_META)).withAvroCoders(p);

      AvroIO.Write<MetadataRecord> write =
          AvroIO.write(MetadataRecord.class)
              .to(metaPath(options))
              .withSuffix(".avro")
              .withoutSharding();

      p.apply("Create collection from datasetId", Create.of(options.getDatasetId()))
          .apply("Metadata interpretation", metadataTr)
          .get(metadataTr.getDataTag())
          .apply("Metadata Kv2Value", Values.create())
          .apply("Write to avro", write);
    }

    LOG.info("Reading avro files");
    PCollection<ExtendedRecord> verbatimRecords =
      p.apply("Read Avro files", AvroIO.read(ExtendedRecord.class).from(inputFile));

    LOG.info("Filter unique values");
    UniqueOccurrenceIdTransform uniquenessTr = UniqueOccurrenceIdTransform.create().withAvroCoders(p);
    PCollectionTuple uniqueTuple = verbatimRecords.apply(uniquenessTr);
    PCollection<ExtendedRecord> uniquenessRecords = uniqueTuple.get(uniquenessTr.getDataTag());

    LOG.info("Adding interpretations");
    AvroOutputTransform writerTransform = AvroOutputTransform.create(options);

    if (checkType.test(TEMPORAL)) {
      LOG.info("- Adding temporal interpretation");
      TemporalRecordTransform temporalTr = TemporalRecordTransform.create().withAvroCoders(p);
      PCollectionTuple temporalTuple = uniquenessRecords.apply("Temporal interpretation", temporalTr);
      writerTransform.write(temporalTuple, TemporalRecord.class, temporalTr, TEMPORAL);
    }

    if (checkType.test(COMMON)) {
      LOG.info("- Adding common interpretation");
      InterpretedExtendedRecordTransform interpretedTr = InterpretedExtendedRecordTransform.create().withAvroCoders(p);
      PCollectionTuple interpretedTuple = uniquenessRecords.apply("Common interpretation", interpretedTr);
      writerTransform.write(interpretedTuple, InterpretedExtendedRecord.class, interpretedTr, COMMON);
    }

    if (checkType.test(MULTIMEDIA)) {
      LOG.info("- Adding multimedia interpretation");
      MultimediaRecordTransform multimediaTr = MultimediaRecordTransform.create().withAvroCoders(p);
      PCollectionTuple multimediaTuple = uniquenessRecords.apply("Multimedia interpretation", multimediaTr);
      writerTransform.write(multimediaTuple, MultimediaRecord.class, multimediaTr, MULTIMEDIA);
    }

    if (checkType.test(TAXONOMY)) {
      LOG.info("- Adding taxonomy interpretation");
      TaxonRecordTransform taxonTr = TaxonRecordTransform.create(getWsConfig(SPECIES_MATCH2)).withAvroCoders(p);
      PCollectionTuple taxonRecordTuple = uniquenessRecords.apply("Taxonomy interpretation", taxonTr);
      writerTransform.write(taxonRecordTuple, TaxonRecord.class, taxonTr, TAXONOMY);
    }

    if (checkType.test(LOCATION)) {
      LOG.info("- Adding location interpretation");
      LocationRecordTransform locationTr = LocationRecordTransform.create(getWsConfig(GEO_CODE)).withAvroCoders(p);
      PCollectionTuple locationTuple = uniquenessRecords.apply("Location interpretation", locationTr);
      writerTransform.write(locationTuple, LocationRecord.class, locationTr, LOCATION);
    }

    return p.run().waitUntilFinish();
  }

  private String metaPath(DataProcessingPipelineOptions options) {
    return FsUtils.buildPathString(
        options.getTargetPath(),
        options.getDatasetId(),
        options.getAttempt().toString(),
        "metadata");
  }

  private Config getWsConfig(Service service) {
    return HttpConfigFactory.createConfig(service, Paths.get(options.getWsProperties()));
  }
}
