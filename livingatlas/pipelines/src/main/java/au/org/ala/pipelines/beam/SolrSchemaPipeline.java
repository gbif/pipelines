package au.org.ala.pipelines.beam;

import au.org.ala.pipelines.options.SolrPipelineOptions;
import au.org.ala.pipelines.util.VersionInfo;
import au.org.ala.utils.CombinedYamlConfiguration;
import au.org.ala.utils.ValidationUtils;
import java.util.*;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.extensions.avro.io.AvroIO;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.gbif.pipelines.common.beam.options.PipelinesOptionsFactory;
import org.gbif.pipelines.io.avro.*;
import org.slf4j.MDC;

/** Debug pipeline to retrieve a list of indexable fields with types and counts. */
@Slf4j
public class SolrSchemaPipeline {

  public static void main(String[] args) throws Exception {
    VersionInfo.print();
    MDC.put("step", "SOLR_SCHEMA");

    String[] combinedArgs =
        new CombinedYamlConfiguration(args).toArgs("general", "solr", "solrSchema");
    SolrPipelineOptions options =
        PipelinesOptionsFactory.create(SolrPipelineOptions.class, combinedArgs);
    MDC.put("datasetId", options.getDatasetId() != null ? options.getDatasetId() : "ALL_RECORDS");
    options.setMetaFileName(ValidationUtils.INDEXING_METRICS);
    PipelinesOptionsFactory.registerHdfs(options);
    run(options);
  }

  public static List<String> indexRecordToDesc(IndexRecord record) {
    List<String> fieldNameAndTypes = new ArrayList<>();

    fieldNameAndTypes.addAll(
        record.getStrings().keySet().stream()
            .map(name -> name + ",string")
            .collect(Collectors.toList()));
    fieldNameAndTypes.addAll(
        record.getInts().keySet().stream().map(name -> name + ",int").collect(Collectors.toList()));
    fieldNameAndTypes.addAll(
        record.getDates().keySet().stream()
            .map(name -> name + ",date")
            .collect(Collectors.toList()));
    fieldNameAndTypes.addAll(
        record.getLongs().keySet().stream()
            .map(name -> name + ",long")
            .collect(Collectors.toList()));
    fieldNameAndTypes.addAll(
        record.getMultiValues().keySet().stream()
            .map(name -> name + ",multiValue")
            .collect(Collectors.toList()));
    fieldNameAndTypes.addAll(
        record.getBooleans().keySet().stream()
            .map(name -> name + ",boolean")
            .collect(Collectors.toList()));

    return fieldNameAndTypes;
  }

  public static void run(SolrPipelineOptions options) {

    Pipeline pipeline = Pipeline.create(options);

    // Load IndexRecords - keyed on UUID
    PCollection<IndexRecord> indexRecordsCollection = loadIndexRecords(options, pipeline);

    indexRecordsCollection
        .apply(
            "Extract String fields",
            FlatMapElements.into(TypeDescriptors.strings())
                .via(SolrSchemaPipeline::indexRecordToDesc))
        .apply(Count.perElement())
        .apply(
            MapElements.via(
                new SimpleFunction<KV<String, Long>, String>() {
                  @Override
                  public String apply(KV<String, Long> input) {
                    return input.getKey() + "," + input.getValue();
                  }
                }))
        .apply(
            TextIO.write()
                .to(options.getAllDatasetsInputPath() + "/index-record-stats/fields.csv")
                .withoutSharding());

    log.info("Starting pipeline");
    pipeline.run(options).waitUntilFinish();

    log.info("Solr indexing pipeline complete");
  }

  /** Load index records from AVRO. */
  private static PCollection<IndexRecord> loadIndexRecords(
      SolrPipelineOptions options, Pipeline p) {
    if (options.getDatasetId() != null && !"all".equalsIgnoreCase(options.getDatasetId())) {
      return p.apply(
          AvroIO.read(IndexRecord.class)
              .from(
                  String.join(
                      "/",
                      options.getAllDatasetsInputPath(),
                      "index-record",
                      options.getDatasetId() + "/*.avro")));
    }

    return p.apply(
        AvroIO.read(IndexRecord.class)
            .from(String.join("/", options.getAllDatasetsInputPath(), "index-record", "*/*.avro")));
  }
}
