package org.gbif.validator.ws.file;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Path;
import java.util.Optional;
import lombok.SneakyThrows;
import org.gbif.validator.api.FileFormat;
import org.junit.jupiter.api.Test;

/** Unit tests related to {@link MediaTypeAndFormatDetector} */
class MediaTypeAndFormatDetectorTest {

  private Path getTestFile(String file) {
    return new File(MediaTypeAndFormatDetectorTest.class.getResource(file).getFile()).toPath();
  }

  private Path getTestPath(String directory) {
    return new File("src/test/resources/" + directory).toPath();
  }

  @SneakyThrows
  private InputStream getTestFileInputStream(String file) {
    return new FileInputStream(MediaTypeAndFormatDetectorTest.class.getResource(file).getFile());
  }

  @Test
  void testDetectMediaType() throws IOException {
    assertEquals(
        SupportedMediaTypes.APPLICATION_EXCEL,
        MediaTypeAndFormatDetector.detectMediaType(
            getTestFile("/workbooks/occurrence-workbook.xls")));

    assertEquals(
        SupportedMediaTypes.APPLICATION_OFFICE_SPREADSHEET,
        MediaTypeAndFormatDetector.detectMediaType(
            getTestFile("/workbooks/occurrence-workbook.xlsx")));

    assertEquals(
        SupportedMediaTypes.APPLICATION_OPEN_DOC_SPREADSHEET,
        MediaTypeAndFormatDetector.detectMediaType(
            getTestFile("/workbooks/occurrence-workbook.ods")));

    assertEquals(
        SupportedMediaTypes.TEXT_CSV,
        MediaTypeAndFormatDetector.detectMediaType(
            getTestFile("/workbooks/occurrence-workbook.csv")));
  }

  @Test
  void testDetectMediaTypeByStream() throws IOException {
    try (InputStream occurrenceWorkBook =
        getTestFileInputStream("/workbooks/occurrence-workbook.xlsx")) {
      // without hint from the filename that's the best we can get
      assertEquals(
          "application/x-tika-ooxml",
          MediaTypeAndFormatDetector.detectMediaType(occurrenceWorkBook, null));
      // if we give the filename (including the extension) Tika use it to "improve the detail of the
      // detection"
      assertEquals(
          SupportedMediaTypes.APPLICATION_OFFICE_SPREADSHEET,
          MediaTypeAndFormatDetector.detectMediaType(
              occurrenceWorkBook, "occurrence-workbook.xlsx"));
    }
  }

  /** The following tests consider the folder as if it was the result of a zip extraction. */
  @Test
  void testEvaluateMediaTypeAndFormat() throws IOException {

    Path extractedFolder = getTestPath("/dwca/dwca-id-with-term/");
    Optional<MediaTypeAndFormatDetector.MediaTypeAndFormat> mediaTypeAndFormat =
        MediaTypeAndFormatDetector.evaluateMediaTypeAndFormat(
            extractedFolder, org.apache.tika.mime.MediaType.APPLICATION_ZIP.toString());
    assertTrue(mediaTypeAndFormat.isPresent());
    assertEquals(FileFormat.DWCA, mediaTypeAndFormat.get().getFileFormat());

    // if the extracted folder contains only a csv file, we can change the mediaType
    extractedFolder = getTestPath("/tabular/single-file/");
    mediaTypeAndFormat =
        MediaTypeAndFormatDetector.evaluateMediaTypeAndFormat(
            extractedFolder, org.apache.tika.mime.MediaType.APPLICATION_ZIP.toString());
    assertTrue(mediaTypeAndFormat.isPresent());
    assertEquals(FileFormat.TABULAR, mediaTypeAndFormat.get().getFileFormat());
  }
}
