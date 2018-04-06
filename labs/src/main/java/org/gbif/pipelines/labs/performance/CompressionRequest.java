package org.gbif.pipelines.labs.performance;

import java.nio.file.Path;

import org.apache.avro.file.CodecFactory;

/**
 * Request parameters for performing compression test.
 */
public class CompressionRequest {

  private final Path dataset;
  private final int syncInterval;
  private final int repetition;
  private final CodecFactory codec;

  public CompressionRequest(Path dataset, int syncInterval, int repetition, CodecFactory codec) {
    this.dataset = dataset;
    this.syncInterval = syncInterval;
    this.repetition = repetition;
    this.codec = codec;
  }

  /**
   * Path of dataset.
   */
  public Path getDataset() {
    return dataset;
  }

  /**
   * the approximate number of uncompressed bytes to write in each block
   */
  public int getSyncInterval() {
    return syncInterval;
  }

  /**
   * Repetition for compression tests to be performed on provided dataset.
   */
  public int getRepetition() {
    return repetition;
  }

  /**
   * Compression codec to be used.
   */
  public CodecFactory getCodec() {
    return codec;
  }

  @Override
  public String toString() {
    return com.google.common.base.Objects.toStringHelper(this)
      .add("dataset", dataset)
      .add("syncInterval", syncInterval)
      .add("repetition", repetition)
      .add("codec", codec)
      .toString();
  }
}
