package org.gbif.pipelines.common.utils;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Stream;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class PathUtil {

  private static final String TAR_EXT = ".tar.xz";

  /**
   * Input path example - /mnt/auto/crawler/dwca/9bed66b3-4caa-42bb-9c93-71d7ba109dad
   */
  public static Path buildDwcaInputPath(String archiveRepository, UUID dataSetUuid) {
    Path directoryPath = Paths.get(archiveRepository, dataSetUuid.toString());
    if (!directoryPath.toFile().exists()) {
      throw new IllegalStateException("Directory does not exist! - " + directoryPath);
    }
    return directoryPath;
  }

  /**
   * Input path result example, directory - /mnt/auto/crawler/xml/9bed66b3-4caa-42bb-9c93-71d7ba109dad/2,
   * if directory is absent, tries check a tar archive  - /mnt/auto/crawler/xml/9bed66b3-4caa-42bb-9c93-71d7ba109dad/2.tar.xz
   */
  public static Path buildXmlInputPath(String archiveRepository, Set<String> archiveRepositorySubdir, UUID dataSetUuid,
      String attempt) {

    Path directoryPath = archiveRepositorySubdir.stream()
        .map(subdir -> Paths.get(archiveRepository, subdir, dataSetUuid.toString()).toFile())
        .filter(File::exists)
        .findFirst()
        .orElseThrow(() -> new IllegalArgumentException("Can't find directory for dataset - " + dataSetUuid))
        .toPath();

    // Check dir, as an example - /mnt/auto/crawler/xml/9bed66b3-4caa-42bb-9c93-71d7ba109dad/2
    Path sibling = directoryPath.resolve(String.valueOf(attempt));
    if (sibling.toFile().exists()) {
      return sibling;
    }

    // Check dir, as an example - /mnt/auto/crawler/xml/9bed66b3-4caa-42bb-9c93-71d7ba109dad/2.tar.xz
    sibling = directoryPath.resolve(attempt + TAR_EXT);
    if (sibling.toFile().exists()) {
      return sibling;
    }

    try (Stream<Path> walk = Files.list(directoryPath)) {
      String parsedAttempt = walk.map(Path::toFile)
          .map(File::getName)
          .map(name -> name.replace(TAR_EXT, ""))
          .filter(f -> f.matches("[0-9]+"))
          .map(Integer::valueOf)
          .max(Integer::compareTo)
          .map(String::valueOf)
          .orElse("0");

      // Check dir, as an example - /mnt/auto/crawler/xml/9bed66b3-4caa-42bb-9c93-71d7ba109dad/2
      sibling = directoryPath.resolve(parsedAttempt);
      if (sibling.toFile().exists()) {
        return sibling;
      }

      // Check dir, as an example - /mnt/auto/crawler/xml/9bed66b3-4caa-42bb-9c93-71d7ba109dad/2.tar.xz
      sibling = directoryPath.resolve(parsedAttempt + TAR_EXT);
      if (sibling.toFile().exists()) {
        return sibling;
      }
    } catch (IOException ex) {
      log.error(ex.getMessage(), ex);
    }

    // Return general
    return directoryPath;
  }

}
