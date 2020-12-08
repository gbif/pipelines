package au.org.ala.pipelines.beam;

import au.org.ala.pipelines.options.ALASolrPipelineOptions;
import au.org.ala.pipelines.transforms.IndexRecordTransform;
import au.org.ala.pipelines.util.VersionInfo;
import au.org.ala.utils.CombinedYamlConfiguration;
import au.org.ala.utils.ValidationUtils;
import lombok.extern.slf4j.Slf4j;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.AvroIO;
import org.apache.beam.sdk.io.solr.SolrIO;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.gbif.pipelines.common.beam.options.PipelinesOptionsFactory;
import org.gbif.pipelines.io.avro.IndexRecord;
import org.joda.time.Duration;
import org.slf4j.MDC;

/**
 * Simple pipeline that reads IndexRecords and sends them to SOLR. Strictly no joining or additional
 * processing in this pipeline.
 */
@Slf4j
public class IndexPipeline {

  public static void main(String[] args) throws Exception {
    VersionInfo.print();
    MDC.put("step", "INDEX_RECORD_FINAL_TO_SOLR");
    String[] combinedArgs =
        new CombinedYamlConfiguration(args).toArgs("general", "speciesLists", "index");
    ALASolrPipelineOptions options =
        PipelinesOptionsFactory.create(ALASolrPipelineOptions.class, combinedArgs);
    options.setMetaFileName(ValidationUtils.INDEXING_METRICS);
    PipelinesOptionsFactory.registerHdfs(options);
    run(options);
  }

  public static void run(ALASolrPipelineOptions options) {

    Pipeline pipeline = Pipeline.create(options);

    SolrIO.ConnectionConfiguration conn =
        SolrIO.ConnectionConfiguration.create(options.getZkHost());

    PCollection<IndexRecord> indexRecords = loadIndexRecords(options, pipeline);

    indexRecords
        .apply(
            "SOLR_indexRecordsWithSampling",
            ParDo.of(new IndexRecordTransform.IndexRecordToSolrInputDocumentFcn()))
        .apply(
            SolrIO.write()
                .to(options.getSolrCollection())
                .withConnectionConfiguration(conn)
                .withMaxBatchSize(options.getSolrBatchSize())
                .withRetryConfiguration(
                    SolrIO.RetryConfiguration.create(10, Duration.standardMinutes(3))));
    pipeline.run(options).waitUntilFinish();

    log.info("Solr indexing pipeline complete");
  }

  private static PCollection<IndexRecord> loadIndexRecords(
      ALASolrPipelineOptions options, Pipeline p) {
    return p.apply(
        AvroIO.read(IndexRecord.class)
            .from(
                String.join(
                    "/", options.getAllDatasetsInputPath(), "index-record-final", "*.avro")));
  }
}
