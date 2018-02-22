package org.gbif.pipelines.hadoop.io;

import org.gbif.pipelines.core.io.DwCAReader;
import org.gbif.pipelines.io.avro.ExtendedRecord;

import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.util.UUID;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Presents the DwCA star records as ExtendedRecords.
 * The core record ID will be used or if not present, a UUID is created.
 */
public class DwCAInputFormat extends FileInputFormat<Text, ExtendedRecord> {

  private static final int BUFFER_COPY_SIZE = 10_000;
  private static final Logger LOG = LoggerFactory.getLogger(DwCAInputFormat.class);

  @Override
  protected boolean isSplitable(JobContext context, Path filename) {
    return false;
  }

  @Override
  public RecordReader<Text, ExtendedRecord> createRecordReader(
    InputSplit inputSplit, TaskAttemptContext taskAttemptContext
  ) throws IOException, InterruptedException {
    return new ExtendedRecordReader();
  }

  /**
   * A simple wrapper that presents ExtendedRecords as key-value pairs for Hadoop formats.
   */
  static class ExtendedRecordReader extends RecordReader<Text, ExtendedRecord> {
    private DwCAReader reader;
    private long recordsReturned;

    @Override
    public void initialize(InputSplit inputSplit, TaskAttemptContext context)
      throws IOException, InterruptedException {

      // what follows is very questionable but a quick test
      // the file is read from HDFS and copied to a temporary location
      FileSplit split = (FileSplit)inputSplit;
      Configuration job = context.getConfiguration();
      Path file = split.getPath();
      FileSystem fs = file.getFileSystem(job);
      java.nio.file.Path tmpFile = Files.createTempFile("tmp", ".zip"); // consider using job and task IDs?
      FSDataInputStream fileIn = fs.open(file);
      FileOutputStream fileOut = new FileOutputStream(tmpFile.toFile());
      LOG.info("Copying from {} to {}", file, tmpFile);
      IOUtils.copyBytes(fileIn, fileOut, BUFFER_COPY_SIZE, true);

      // having copied the file out of HDFS onto the local FS in a temp folder, we prepare it (sorts files)
      java.nio.file.Path tmpSpace = Files.createTempDirectory("tmp-" + context.getTaskAttemptID().getJobID().getId() +
                                                              ":" + context.getTaskAttemptID().getId());

      reader = new DwCAReader(tmpFile.toAbsolutePath().toString(), tmpSpace.toAbsolutePath().toString());
      nextKeyValue();
    }

    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
      return reader.advance();
    }

    @Override
    public Text getCurrentKey() throws IOException, InterruptedException {
      ExtendedRecord e = reader.getCurrent();
      if (e!=null) {
        // use the DwC record ID or else a UUID
        // TODO: consider this... ID is no good if it is not unique within the file
        String id = e.getId() == null ? UUID.randomUUID().toString() : e.getId();
        return new Text(id);
      }
      throw new IllegalStateException("Cannot get current key when no current record is available");
    }

    @Override
    public ExtendedRecord getCurrentValue() throws IOException, InterruptedException {
      return reader.getCurrent();
    }

    @Override
    public float getProgress() throws IOException, InterruptedException {
      return 0.5f; // Without access to size or total count no way to be truthful
    }

    @Override
    public void close() throws IOException {
      reader.close();
    }
  }
}
