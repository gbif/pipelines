package org.gbif.converters.converter;

import java.io.IOException;
import java.net.URI;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.hadoop.fs.AvroFSInput;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.gbif.pipelines.io.avro.ExtendedRecord;

@Slf4j
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class FsUtils {

  private static final long FILE_LIMIT_SIZE = 3L * 1_024L; // 3Kb

  /**
   * Helper method to create a parent directory in the provided path
   *
   * @return filesystem
   */
  @SneakyThrows
  public static FileSystem createParentDirectories(
      String hdfsSiteConfig, String coreSiteConfig, Path path) {
    FileSystem fs =
        FileSystemFactory.getInstance(hdfsSiteConfig, coreSiteConfig).getFs(path.toString());
    fs.mkdirs(path.getParent());
    return fs;
  }

  /**
   * If a file is too small (less than 3Kb), checks any records inside, if the file is empty,
   * removes it
   */
  @SneakyThrows
  public static boolean deleteAvroFileIfEmpty(FileSystem fs, Path path) {
    if (!fs.exists(path)) {
      return true;
    }

    if (fs.getFileStatus(path).getLen() > FILE_LIMIT_SIZE) {
      return false;
    }

    SpecificDatumReader<ExtendedRecord> datumReader =
        new SpecificDatumReader<>(ExtendedRecord.class);
    try (AvroFSInput input = new AvroFSInput(fs.open(path), fs.getFileStatus(path).getLen());
        DataFileReader<ExtendedRecord> dataFileReader = new DataFileReader<>(input, datumReader)) {
      if (!dataFileReader.hasNext()) {
        log.warn("File is empty - {}", path);
        Path parent = path.getParent();
        fs.delete(parent, true);

        Path subParent = parent.getParent();
        if (!fs.listFiles(subParent, true).hasNext()) {
          fs.delete(subParent, true);
        }
        return true;
      }
      return false;
    }
  }

  public static long fileSize(String hdfsSiteConfig, String coreSiteConfig, URI file)
      throws IOException {
    FileSystem fs =
        FileSystemFactory.getInstance(hdfsSiteConfig, coreSiteConfig).getFs(file.toString());
    return fs.getFileStatus(new Path(file)).getLen();
  }

  public static void createFile(FileSystem fs, Path path, String body) throws IOException {
    try (FSDataOutputStream stream = fs.create(path, true)) {
      stream.writeChars(body);
    }
  }
}
