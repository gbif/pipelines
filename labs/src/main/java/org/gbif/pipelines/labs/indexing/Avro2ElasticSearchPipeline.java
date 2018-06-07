package org.gbif.pipelines.labs.indexing;

import org.gbif.pipelines.config.EsProcessingPipelineOptions;
import org.gbif.pipelines.io.avro.ExtendedRecord;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;

import com.google.common.base.Throwables;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.io.AvroIO;
import org.apache.beam.sdk.io.elasticsearch.ElasticsearchIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.htrace.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A pipeline to build ElasticSearch from Avro files containing ExtendedRecord data.
 * <p>
 * The core fields are extracted and the simple name is used for the ES fields (no namespace).  A location
 * field is added suitable for `geopoint` indexing.
 * <p>
 * NOTE: This is a transient piece of code and will be removed when the interpretation pipelines are available.
 * As such code contains hard coded values and is not suitable for production use.
 */
public class Avro2ElasticSearchPipeline {

  private static final Logger LOG = LoggerFactory.getLogger(Avro2ElasticSearchPipeline.class);

  private static final String DECIMAL_LATITUDE = "decimalLatitude";
  private static final String DECIMAL_LONGITUDE = "decimalLongitude";
  private static final String LOCATION = "location";

  private static final String SLASH_CONST = "/";

  public static void main(String[] args) {

    EsProcessingPipelineOptions options =
      PipelineOptionsFactory.fromArgs(args).create().as(EsProcessingPipelineOptions.class);
    Pipeline p = Pipeline.create(options);

    // Read Avro files
    PCollection<ExtendedRecord> verbatimStream =
      p.apply("Read Avro files", AvroIO.read(ExtendedRecord.class).from(options.getInputFile()));

    // Convert to JSON
    PCollection<String> jsonInputStream = verbatimStream.apply("Convert to JSON", ParDo.of(new RecordFormatter()));

    // Index in ES
    jsonInputStream.apply(ElasticsearchIO.write()
                            .withConnectionConfiguration(ElasticsearchIO.ConnectionConfiguration.create(options.getESAddresses(),
                                                                                                        options.getESIndexPrefix(),
                                                                                                        options.getESIndexPrefix()))
                            .withMaxBatchSize(options.getESMaxBatchSize()));

    // instruct the writer to use a provided document ID
    LOG.info("Starting the pipeline");
    PipelineResult result = p.run();
    result.waitUntilFinish();
    LOG.info("Pipeline finished with state: {} ", result.getState());
  }

  /**
   * Returns a flat JSON String holding the core records only stripped of namespaces and with the addition of a
   * location field suitable for geopoint indexing in ElasticSearch.
   */
  static class RecordFormatter extends DoFn<ExtendedRecord, String> {

    @ProcessElement
    public void processElement(ProcessContext c) {
      ExtendedRecord record = c.element();
      Map<String, String> terms = record.getCoreTerms();
      Map<String, String> stripped = new HashMap<>(record.getCoreTerms().size());

      Function<String, String> stripNS = source -> source.substring(source.lastIndexOf(SLASH_CONST) + 1);
      terms.forEach((k, v) -> stripped.put(stripNS.apply(k), v));

      // location suitable for geopoint format
      if (Objects.nonNull(stripped.get(DECIMAL_LATITUDE)) && Objects.nonNull(stripped.get(DECIMAL_LONGITUDE))) {
        stripped.put(LOCATION, stripped.get(DECIMAL_LATITUDE).concat(",").concat(stripped.get(DECIMAL_LONGITUDE)));
      }

      try {
        c.output(new ObjectMapper().writeValueAsString(stripped));
      } catch (Exception e) {
        throw Throwables.propagate(e);
      }
    }
  }

}
