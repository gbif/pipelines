package org.gbif.pipelines.core.utils;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Set;
import java.util.stream.Collectors;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import org.gbif.dwc.Archive;
import org.gbif.dwc.DwcFiles;

@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class DwcaExtensionTermUtils {

  public static Set<String> fromLocation(Path path) throws IOException {
    Archive archive = DwcFiles.fromLocation(path);
    return readExtensionTerms(archive);
  }

  public static Set<String> fromCompressed(Path path, Path workingDir) throws IOException {
    Archive archive = DwcFiles.fromCompressed(path, workingDir);
    return readExtensionTerms(archive);
  }

  private static Set<String> readExtensionTerms(Archive archive) {
    return archive.getExtensions().stream()
        .map(x -> x.getRowType().qualifiedName())
        .collect(Collectors.toSet());
  }
}
