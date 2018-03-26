package org.gbif.pipelines.labs.performance;

import java.math.BigDecimal;
import java.nio.file.Path;
import java.text.DecimalFormat;
import java.util.LongSummaryStatistics;

import com.google.common.base.Objects;
import org.apache.avro.file.CodecFactory;

/**
 * Performance Result of 1 unique set of (dataset,codec,syncInterval) tuple after the provided repetition.
 */
public class CompressionResult {

  private final Path dataset;
  private final int syncInterval;
  private final long originalSize;
  private final CodecFactory codecFactoryType;
  private final LongSummaryStatistics compressedSize = new LongSummaryStatistics();
  private final LongSummaryStatistics readTime = new LongSummaryStatistics();
  private final LongSummaryStatistics writeTime = new LongSummaryStatistics();
  private int count = 0;
  private int noOfOccurrence = 0;

  public CompressionResult(Path dataset, int syncInterval, long originalSize, CodecFactory codecFactory) {
    this.dataset = dataset;
    this.syncInterval = syncInterval;
    this.originalSize = originalSize;
    this.codecFactoryType = codecFactory;
  }

  /**
   * Update reading for read,write and compressed file size after each run.
   */
  public void updateReadings(long nxtReadTime, long nxtWriteTime, long nxtCompressedSize) {
    ++count;
    readTime.accept(nxtReadTime);
    writeTime.accept(nxtWriteTime);
    compressedSize.accept(nxtCompressedSize);
  }

  /**
   * Path of dataset.
   */
  public Path getDataset() {
    return dataset;
  }

  /**
   * SyncInterval of the configuration.
   */
  public int getSyncInterval() {
    return syncInterval;
  }

  /**
   * file size of original DwC expanded path.(It is a folder size)
   */
  public long getOriginalSize() {
    return originalSize;
  }

  /**
   * average size of compressed file after {@link #getTotalObservations()} repetitions.
   */
  public double getAvgCompressedSize() {
    return compressedSize.getAverage();
  }

  /**
   * average time to read created avro file.
   */
  public double getAvgReadTime() {
    return readTime.getAverage();
  }

  /**
   * average time to write DwC expanded file to avro with provided configuration.
   */
  public double getAvgWriteTime() {
    return writeTime.getAverage();
  }

  /**
   * Number of repetition of test.
   */
  public int getTotalObservations() {
    return count;
  }

  /**
   * Codec for compression of records.
   */
  public CodecFactory getCodecFactoryType() {
    return codecFactoryType;
  }

  /**
   * Number of occurrence.
   */
  public int getNoOfOccurrence() {
    return noOfOccurrence;
  }

  /**
   * Set number of occurrence.
   */
  public void setNoOfOccurrence(int noOfOccurrence) {
    this.noOfOccurrence = noOfOccurrence;
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this)
      .add("dataset", dataset)
      .add("syncInterval", syncInterval)
      .add("totalObservations", count)
      .add("originalSize(in bytes)", originalSize)
      .add("compressedSize(in bytes)", compressedSize.getAverage())
      .add("originalFormattedFileSize(approx)", getFormattedFileSize(BigDecimal.valueOf(originalSize)))
      .add("compressedFormattedFileSize(approx)", getFormattedFileSize(BigDecimal.valueOf(compressedSize.getAverage())))
      .add("readTime(in ms)", readTime.getAverage())
      .add("writeTime(in ms)", writeTime.getAverage())
      .add("codecFactory", codecFactoryType)
      .add("noOfOccurrence", noOfOccurrence)
      .toString();
  }

  public String toCSV() {
    return dataset.toFile().getName()
           + ","
           + syncInterval
           + ","
           + count
           + ","
           + originalSize
           + ","
           + compressedSize.getAverage()
           + ","
           + getFormattedFileSize(BigDecimal.valueOf(originalSize))
           + ","
           + getFormattedFileSize(BigDecimal.valueOf(compressedSize.getAverage()))
           + ","
           + readTime.getAverage()
           + ","
           + writeTime.getAverage()
           + ","
           + codecFactoryType
           + ","
           + noOfOccurrence;
  }

  /**
   * Formats file size with KBs,MBs,GBs suffix.
   */
  private String getFormattedFileSize(BigDecimal size) {
    String hrSize;
    BigDecimal k = size.divide(BigDecimal.valueOf(1024.0));
    BigDecimal m = size.divide(BigDecimal.valueOf(1024.0 * 1024.0));
    BigDecimal g = size.divide(BigDecimal.valueOf(1024.0 * 1024.0 * 1024.0));
    DecimalFormat dec = new DecimalFormat("0.00");

    if (g.floatValue() > 1) {
      hrSize = dec.format(g).concat("GB");
    } else if (m.floatValue() > 1) {
      hrSize = dec.format(m).concat("MB");
    } else {
      hrSize = dec.format(size).concat("KB");
    }
    return hrSize;
  }

}
