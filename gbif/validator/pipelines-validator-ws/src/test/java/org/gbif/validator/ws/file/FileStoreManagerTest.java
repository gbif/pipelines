package org.gbif.validator.ws.file;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.io.File;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import lombok.SneakyThrows;
import org.gbif.validator.api.FileFormat;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;
import org.mockserver.integration.ClientAndServer;
import org.mockserver.junit.jupiter.MockServerExtension;
import org.springframework.mock.web.MockMultipartFile;

/** {@link FileStoreManager} tests. */
@ExtendWith(MockServerExtension.class)
class FileStoreManagerTest extends DownloadFileBaseTest {

  // Directory used as temporary space to upload files
  @TempDir Path workingDirectory;

  // Directory used as files store
  @TempDir Path storeDirectory;

  public FileStoreManagerTest(ClientAndServer clientAndServer) {
    super(clientAndServer);
  }

  @SneakyThrows
  @Test
  void uploadXmlMultiPartFileTest() {

    ExecutionException thrown =
        Assertions.assertThrows(
            ExecutionException.class,
            () -> uploadTest("/xml/", "abcd2.xml", "application/xml", FileFormat.XML));

    Assertions.assertEquals(
        "Unsupported file type: application/xml", thrown.getCause().getMessage());
  }

  @SneakyThrows
  @Test
  void uploadMultiPartFileTest() {
    uploadTest("/dwca/", "Archive.zip", "application/zip", FileFormat.DWCA);
  }

  /** Generic method to test file uploads. */
  @SneakyThrows
  public void uploadTest(
      String filePath, String fileName, String contentType, FileFormat fileFormat) {
    try (InputStream archive = readTestFileInputStream(filePath + fileName)) {
      MockMultipartFile mockMultipartFile =
          new MockMultipartFile("file", fileName, contentType, archive);
      // directory inside the storeDirectory in which file is upload
      UUID targetDirectory = UUID.randomUUID();

      FileStoreManager fileStoreManager =
          new FileStoreManager(
              workingDirectory.toString(),
              storeDirectory.toString(),
              ctx.getBean(DownloadFileManager.class));

      FileStoreManager.AsyncDataFileTask task =
          fileStoreManager.uploadDataFile(mockMultipartFile, targetDirectory.toString());

      // Wait the task to finish
      DataFile dataFile = task.getTask().get();

      assertTrue(Files.exists(dataFile.getFilePath()));
      assertTrue(dataFile.getSize() > 0);
      assertEquals(fileName, dataFile.getSourceFileName());
      assertEquals(fileFormat, dataFile.getFileFormat());
      assertEquals(contentType, dataFile.getMediaType());
      assertEquals(contentType, dataFile.getReceivedAsMediaType());
    }
  }

  @SneakyThrows
  @Test
  void downloadZipFileTest() {
    // State
    FileStoreManager fileStoreManager =
        new FileStoreManager(
            workingDirectory.toString(),
            storeDirectory.toString(),
            ctx.getBean(DownloadFileManager.class));
    UUID key = UUID.randomUUID();

    // When
    FileStoreManager.AsyncDownloadResult downloadResult =
        fileStoreManager.downloadDataFile(
            testPath("/Archive.zip"), key.toString(), System.out::println, err -> fail());

    // Should
    File downloadFile = downloadResult.getDownloadTask().get();
    Path parent = downloadFile.toPath().getParent();

    assertTrue(Files.exists(parent.resolve("eml.xml")));
    assertTrue(Files.exists(parent.resolve("meta.xml")));
    assertTrue(Files.exists(parent.resolve("occurrence.txt")));
    assertEquals("Archive.zip", downloadResult.getDataFile().getSourceFileName());
  }
}
