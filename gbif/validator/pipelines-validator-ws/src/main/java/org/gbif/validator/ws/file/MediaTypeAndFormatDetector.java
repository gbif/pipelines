package org.gbif.validator.ws.file;

import static org.gbif.validator.ws.file.SupportedMediaTypes.COMPRESS_CONTENT_TYPE;
import static org.gbif.validator.ws.file.SupportedMediaTypes.SPREADSHEET_CONTENT_TYPES;
import static org.gbif.validator.ws.file.SupportedMediaTypes.TABULAR_CONTENT_TYPES;

import jakarta.annotation.Nullable;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import org.apache.tika.Tika;
import org.gbif.validator.api.FileFormat;

/**
 * Utility class to: - automatically detect the media types based on file or bytes. - decide
 * org.gbif.validation.api.vocabulary.FileFormat based on media type
 */
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class MediaTypeAndFormatDetector {

  private static final Tika TIKA = new Tika();

  /**
   * @see org.apache.tika.detect.Detector
   * @return detected media type
   */
  public static String detectMediaType(Path filePath) throws IOException {
    return TIKA.detect(filePath);
  }

  /**
   * Detected media type from the an {@link InputStream} and a filename. "Firstly, magic based
   * detection is used on the start of the file ... if available, the filename is then used to
   * improve the detail of the detection" Source: https://tika.apache.org/1.1/detection.html
   *
   * @param filename filename including the extension. Optional but used to improve the detail of
   *     the detection.
   * @see org.apache.tika.detect.Detector
   */
  public static String detectMediaType(InputStream is, @Nullable String filename)
      throws IOException {
    return TIKA.detect(is, filename);
  }

  /**
   * Given a {@link Path} to a file (or folder) and a original contentType this function will check
   * to reevaluate the contentType and return the matching {@link FileFormat}. If a more specific
   * contentType can not be found the original one will be return with the matching {@link
   * FileFormat}.
   *
   * @param dataFilePath shall point to data file or folder (not a zip file)
   */
  public static Optional<MediaTypeAndFormat> evaluateMediaTypeAndFormat(
      Path dataFilePath, String detectedContentType) throws IOException {
    Objects.requireNonNull(dataFilePath, "dataFilePath must be provided");
    String contentType = detectedContentType;

    if (COMPRESS_CONTENT_TYPE.contains(detectedContentType)) {
      List<Path> content;
      try (Stream<Path> list = Files.list(dataFilePath)) {
        content = list.collect(Collectors.toList());
      }
      if (content.size() == 1) {
        contentType = MediaTypeAndFormatDetector.detectMediaType(content.get(0));
      } else {
        return Optional.of(MediaTypeAndFormat.create(contentType, FileFormat.DWCA));
      }
    }

    if (TABULAR_CONTENT_TYPES.contains(contentType)) {
      return Optional.of(MediaTypeAndFormat.create(contentType, FileFormat.TABULAR));
    }

    if (SPREADSHEET_CONTENT_TYPES.contains(contentType)) {
      return Optional.of(MediaTypeAndFormat.create(contentType, FileFormat.SPREADSHEET));
    }
    return Optional.empty();
  }

  /** Simple holder for mediaType and fileFormat */
  @Getter
  @AllArgsConstructor(staticName = "create")
  public static class MediaTypeAndFormat {
    private final String mediaType;
    private final FileFormat fileFormat;
  }
}
