package org.gbif.data.pipelines.io.dwca.hdfs;

import org.gbif.data.io.avro.ExtendedRecord;
import org.gbif.data.pipelines.io.dwca.ExtendedRecords;
import org.gbif.dwc.DwcFiles;
import org.gbif.dwc.NormalizedDwcArchive;
import org.gbif.dwca.io.Archive;
import org.gbif.dwca.record.StarRecord;
import org.gbif.utils.file.ClosableIterator;

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



  static class ExtendedRecordReader extends RecordReader<Text, ExtendedRecord> {
    private ClosableIterator<StarRecord> iter;
    private long recordsReturned;
    private ExtendedRecord current;

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
      IOUtils.copyBytes(fileIn, fileOut, 100000, true);

      // having copied the file out of HDFS onto the local FS in a temp folder, we prepare it (sorts files)
      java.nio.file.Path tmpSpace = Files.createTempDirectory("tmp-" + context.getTaskAttemptID().getJobID().getId() +
                                                              ":" + context.getTaskAttemptID().getId());
      Archive dwcArchive = DwcFiles.fromCompressed(tmpFile, tmpSpace);
      NormalizedDwcArchive nda = DwcFiles.prepareArchive(dwcArchive, false, false);
      iter = nda.iterator();
      nextKeyValue();
    }

    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
      if (iter==null || !iter.hasNext()) {
        return false;
      }
      final StarRecord next = iter.next();
      recordsReturned++;
      if (recordsReturned % 1000 == 0) {
        LOG.info("Read [{}] records", recordsReturned);
      }
      current = ExtendedRecords.newFromStarRecord(next);

      // TODO: consider this... the core ID might not be unique
      current.setId(UUID.randomUUID().toString());
      return true;
    }

    @Override
    public Text getCurrentKey() throws IOException, InterruptedException {
      // use the DwC record ID or else a UUID
      // TODO: consider this... ID is no good if it is not unique within the file
      String id = current.getId() == null ? UUID.randomUUID().toString() : current.getId().toString();
      return new Text(id);
    }

    @Override
    public ExtendedRecord getCurrentValue() throws IOException, InterruptedException {
      current.setId(getCurrentKey().toString()); // override the ID
      return current;
    }

    @Override
    public float getProgress() throws IOException, InterruptedException {
      return 0.5f; // what else?
    }

    @Override
    public void close() throws IOException {
      if (iter!=null) {
        try {
          LOG.info("Closing DwC-A reader having read [{}] records", recordsReturned);
          iter.close();
        } catch (Exception e) {
          throw new IOException(e);
        }
      }
    }
  }
}
