package org.gbif.converters.converter;

import org.gbif.pipelines.io.avro.ExtendedRecord;

import java.io.File;
import java.io.IOException;
import java.net.URI;

import com.google.common.base.Strings;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.AvroFSInput;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FsUtils {

  private static final Logger LOG = LoggerFactory.getLogger(FsUtils.class);

  private static final long FILE_LIMIT_SIZE = 3L * 1_024L; //3Kb

  private FsUtils() {
  }

  /**
   * Helper method to get file system based on provided configuration.
   */
  public static FileSystem getFileSystem(String extendedRecordRepository, String hdfsSiteConfig) {
    try {
      Configuration config = new Configuration();

      // check if the hdfs-site.xml is provided
      if (!Strings.isNullOrEmpty(hdfsSiteConfig)) {
        File hdfsSite = new File(hdfsSiteConfig);
        if (hdfsSite.exists() && hdfsSite.isFile()) {
          LOG.info("using hdfs-site.xml");
          config.addResource(hdfsSite.toURI().toURL());
        } else {
          LOG.warn("hdfs-site.xml does not exist");
        }
      }

      return FileSystem.get(URI.create(extendedRecordRepository), config);
    } catch (IOException ex) {
      throw new IllegalStateException("Can't get a valid filesystem from provided uri " + extendedRecordRepository, ex);
    }
  }

  /**
   * Helper method to create a parent directory in the provided path
   *
   * @return filesystem
   */
  public static FileSystem createParentDirectories(Path extendedRepoPath, String hdfsSite) {
    FileSystem fs = getFileSystem(extendedRepoPath.toString(), hdfsSite);
    try {
      fs.mkdirs(extendedRepoPath.getParent());
    } catch (IOException e) {
      throw new IllegalStateException("Error creating parent directories for extendedRepoPath: " + extendedRepoPath, e);
    }
    return fs;
  }

  /**
   * If a file is too small (less than 3Kb), checks any records inside, if the file is empty, removes it
   */
  public static boolean deleteAvroFileIfEmpty(FileSystem fs, Path path) {
    try {
      if (!fs.exists(path)) {
        return true;
      }
      if (fs.getFileStatus(path).getLen() > FILE_LIMIT_SIZE) {
        return false;
      }
      SpecificDatumReader<ExtendedRecord> datumReader = new SpecificDatumReader<>(ExtendedRecord.class);
      try (AvroFSInput input = new AvroFSInput(fs.open(path), fs.getFileStatus(path).getLen());
           DataFileReader<ExtendedRecord> dataFileReader = new DataFileReader<>(input, datumReader)) {
        if (!dataFileReader.hasNext()) {
          LOG.warn("File is empty - {}", path);
          Path parent = path.getParent();
          fs.delete(parent, true);

          Path subParent = parent.getParent();
          if (!fs.listFiles(subParent, true).hasNext()) {
            fs.delete(subParent, true);
          }
          return true;
        }
      }
      return false;
    } catch (IOException ex) {
      LOG.error("Error deleting an empty file", ex);
      throw new IllegalStateException("Error deleting an empty file", ex);
    }
  }

  public static long fileSize(URI file, String hdfsSiteConfig) throws IOException {
    FileSystem fs = getFileSystem(file.toString(), hdfsSiteConfig);
    return fs.getFileStatus(new Path(file)).getLen();
  }

}
