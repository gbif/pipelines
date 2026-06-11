package org.gbif.pipelines.spark.dwcdp;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.gbif.pipelines.spark.util.MapperUtil;

/**
 * Reads a DwC-DP {@code datapackage.json} descriptor from HDFS (or local filesystem as fallback for
 * local development).
 */
@Slf4j
public class DataPackageDescriptorReader {

  private static final ObjectMapper MAPPER = MapperUtil.MAPPER;

  private DataPackageDescriptorReader() {}

  public static DwcDpVerbatimConverter.DataPackage read(FileSystem fileSystem, String path)
      throws IOException {

    Path hdfsPath = new Path(path);
    if (fileSystem.exists(hdfsPath)) {
      log.debug("Reading datapackage.json from HDFS: {}", path);
      try (var inputStream = fileSystem.open(hdfsPath)) {
        return MAPPER.readValue(
            inputStream.getWrappedStream(), DwcDpVerbatimConverter.DataPackage.class);
      }
    }

    // Local filesystem fallback for local development
    java.nio.file.Path localPath = Paths.get(path);
    if (Files.exists(localPath)) {
      log.debug("Reading datapackage.json from local path: {}", path);
      return MAPPER.readValue(localPath.toFile(), DwcDpVerbatimConverter.DataPackage.class);
    }

    throw new IOException("datapackage.json not found at: " + path);
  }
}
