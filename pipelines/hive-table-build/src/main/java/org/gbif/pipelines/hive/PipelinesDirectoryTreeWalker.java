package org.gbif.pipelines.hive;

import java.io.IOException;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.stream.Collectors;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class PipelinesDirectoryTreeWalker {

  private final Path roothPath;
  private final FileSystem fileSystem;

  public PipelinesDirectoryTreeWalker(Path roothPath, FileSystem fileSystem) {
    this.roothPath = roothPath;
    this.fileSystem = fileSystem;
  }

  private Optional<UUID> datasetKeyFromPath(FileStatus fileStatus) {
    if (fileStatus.isDirectory()) {
      try {
        return Optional.of(UUID.fromString(fileStatus.getPath().getName()));
      } catch (IllegalArgumentException ex) {
        return Optional.empty();
      }
    }
    return Optional.empty();
  }

  private Optional<FileStatus> getLatestAttempt(Path datasetPath) {
    try {
      return Arrays.stream(fileSystem.listStatus(datasetPath))
              .max(Comparator.comparingLong(path -> Long.parseLong(path.getPath().getName())));
    } catch (IOException ex) {
      return Optional.empty();
    }
  }

  private Map<UUID, FileStatus> interpretations() throws IOException {
    return Arrays.stream(fileSystem.listStatus(roothPath))
              .map(fileStatus -> datasetKeyFromPath(fileStatus))
              .filter(Optional::isPresent)
              .collect(Collectors.toMap(datasetKey -> datasetKey.get(), datasetKey -> getLatestAttempt(new Path(fileStatus.getPath(),"interpreted"))));
  }

  public static void main(String[] args) throws IOException {
    Configuration config = new Configuration();

    config.addResource(new Path("file://Users/xrc439/dev/gbif/pipelines/pipelines/hive-table-build/src/main/resources/hdfs-site.xml"));
    FileSystem fileSystem = getFileSystem("hdfs://ha-nn/");
    PipelinesDirectoryTreeWalker walker = new PipelinesDirectoryTreeWalker(new Path("/data/ingest/"), fileSystem);


  }

  public static FileSystem getFileSystem(String hdfsNameNode) {
    try {
      Configuration configuration = new Configuration();
      configuration.set(CommonConfigurationKeysPublic.FS_DEFAULT_NAME_KEY, hdfsNameNode);
      return FileSystem.get(configuration);
    } catch (IOException ex) {
      throw new IllegalStateException(ex);
    }

  }
}
