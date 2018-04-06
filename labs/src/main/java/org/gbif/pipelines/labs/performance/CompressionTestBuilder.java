package org.gbif.pipelines.labs.performance;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.function.Function;

import org.apache.avro.file.CodecFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Compression Performance Test Builder, which takes inputs to configure tests and execute it.
 */
public class CompressionTestBuilder {

  private static final Logger LOG = LoggerFactory.getLogger(CompressionTestBuilder.class);

  private final List<Path> datasets;
  private List<Integer> syncIntervals;
  private int repeatTests = 1;
  private List<CodecFactory> codecFactories;

  /**
   * Path of datasets.
   */
  public static CompressionTestBuilder withDatasets(Path[] datasets) {
    return new CompressionTestBuilder(Arrays.asList(datasets));
  }

  private CompressionTestBuilder(List<Path> datasets) {
    this.datasets = datasets;
  }

  /**
   * list of syncIntervals to try all pair of datasets,codec combinations.
   */
  public CompressionTestBuilder withSyncIntervals(Integer[] syncIntervals) {
    this.syncIntervals = Arrays.asList(syncIntervals);
    return this;
  }

  /**
   * Repeat tests for every unique combination of datasets , codecfactory and syncInterval tuple, to get average results.
   */
  public CompressionTestBuilder times(int repeatTests) {
    this.repeatTests = repeatTests;
    return this;
  }

  /**
   * list of codecs to apply for the pairs of dataset.
   */
  public CompressionTestBuilder withEach(CodecFactory[] codecFactories) {
    this.codecFactories = Arrays.asList(codecFactories);
    return this;
  }

  /**
   * Execute compression function and get compression results.
   */
  public List<CompressionResult> performTestUsing(Function<CompressionRequest, CompressionResult> datasetFunction) {
    List<CompressionResult> results = new ArrayList<>();
    datasets.forEach(datasets -> codecFactories.forEach(codecFactory -> syncIntervals.forEach(syncIntervals -> results.add(
      datasetFunction.apply(new CompressionRequest(datasets, syncIntervals, repeatTests, codecFactory))))));
    return results;
  }

  private List<Path> getDatasets() {
    return datasets;
  }

  private List<Integer> getSyncIntervals() {
    return syncIntervals;
  }

  private int getRepeatTests() {
    return repeatTests;
  }

  private List<CodecFactory> getCodecFactories() {
    return codecFactories;
  }
}
