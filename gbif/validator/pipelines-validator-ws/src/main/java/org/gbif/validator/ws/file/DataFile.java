package org.gbif.validator.ws.file;

import java.nio.file.Path;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.ToString;
import org.gbif.validator.api.FileFormat;

@Data
@Builder
@AllArgsConstructor(staticName = "create")
@ToString(callSuper = true)
public class DataFile {

  private final Path filePath;
  private final String sourceFileName;
  private FileFormat fileFormat;
  private String receivedAsMediaType;
  private String mediaType;

  public Long getSize() {
    if (filePath != null) {
      return filePath.toFile().length();
    }
    return null;
  }
}
