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
   * @param datasets
   * @return
   */
  public static CompressionTestBuilder forAll(Path[] datasets) {
    return new CompressionTestBuilder(Arrays.asList(datasets));
  }

  private CompressionTestBuilder(List<Path> datasets) {
    this.datasets = datasets;
  }

  /**
   * list of syncIntervals to try all pair of datasets,codec combinations.
   * @param syncIntervals
   * @return
   */
  public CompressionTestBuilder forEach(Integer[] syncIntervals) {
    this.syncIntervals = Arrays.asList(syncIntervals);
    return this;
  }

  /**
   * Repeat tests for every unique combination of datasets , codecfactory and syncInterval tuple, to get average results.
   * @param repeatTests
   * @return
   */
  public CompressionTestBuilder times(int repeatTests) {
    this.repeatTests = repeatTests;
    return this;
  }

  /**
   * list of codecs to apply for the pairs of dataset.
   * @param codecFactories
   * @return
   */
  public CompressionTestBuilder withEach(CodecFactory[] codecFactories) {
    this.codecFactories = Arrays.asList(codecFactories);
    return this;
  }

  /**
   * Execute compression function and get compression results.
   * @param datasetFunction
   * @return
   */
  public List<CompressionResult> performTestUsing(Function<CompressionRequest, CompressionResult> datasetFunction) {
    List<CompressionResult> results = new ArrayList<>();
    List<CodecFactory> codecFactories = this.getCodecFactories();
    List<Integer> syncIntervals = this.getSyncIntervals();
    List<Path> datasets = this.getDatasets();
    for (int i = 0; i < datasets.size(); i++) {
      for (int j = 0; j < codecFactories.size(); j++) {
        for (int k = 0; k < syncIntervals.size(); k++) {
          results.add(datasetFunction.apply(new CompressionRequest(datasets.get(i),
                                                            syncIntervals.get(k),
                                                            this.getRepeatTests(),
                                                            codecFactories.get(j))));
        }
      }
    }
    return results;
  }

  public List<Path> getDatasets() {
    return datasets;
  }

  public List<Integer> getSyncIntervals() {
    return syncIntervals;
  }

  public int getRepeatTests() {
    return repeatTests;
  }

  public List<CodecFactory> getCodecFactories() {
    return codecFactories;
  }
}
