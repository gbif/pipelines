package org.gbif.validator.ws.file;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.io.File;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.UUID;
import lombok.SneakyThrows;
import org.gbif.validator.api.FileFormat;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;
import org.mockserver.integration.ClientAndServer;
import org.mockserver.junit.jupiter.MockServerExtension;
import org.springframework.mock.web.MockMultipartFile;

/** {@link UploadFileManager} tests. */
@ExtendWith(MockServerExtension.class)
public class UploadFileManagerTest extends DownloadFileBaseTest {

  // Directory used as temporary space to upload files
  @TempDir Path workingDirectory;

  // Directory used as files store
  @TempDir Path storeDirectory;

  public UploadFileManagerTest(ClientAndServer clientAndServer) {
    super(clientAndServer);
  }

  @SneakyThrows
  @Test
  public void uploadXmlMultiPartFileTest() {
    try (InputStream xml = readTestFileInputStream("/xml/abcd2.xml")) {
      // State
      MockMultipartFile mockMultipartFile =
          new MockMultipartFile("file", "abcd2.xml", "application/xml", xml);
      // directory inside the storeDirectory in which file is upload
      UUID targetDirectory = UUID.randomUUID();

      UploadFileManager uploadFileManager =
          new UploadFileManager(
              workingDirectory.toString(),
              storeDirectory.toString(),
              ctx.getBean(DownloadFileManager.class));

      // When
      UploadFileManager.AsyncDataFileTask task =
          uploadFileManager.uploadDataFile(mockMultipartFile, targetDirectory.toString());

      // Wait the task to finish
      DataFile dataFile = task.getTask().get();

      // Should
      assertTrue(Files.exists(dataFile.getFilePath()));
      assertTrue(dataFile.getSize() > 0);
      assertEquals("abcd2.xml", dataFile.getSourceFileName());
      assertEquals(FileFormat.XML, dataFile.getFileFormat());
      assertEquals("application/xml", dataFile.getMediaType());
      assertEquals("application/xml", dataFile.getReceivedAsMediaType());
    }
  }

  @SneakyThrows
  @Test
  public void downloadZipFileTest() {
    // State
    UploadFileManager uploadFileManager =
        new UploadFileManager(
            workingDirectory.toString(),
            storeDirectory.toString(),
            ctx.getBean(DownloadFileManager.class));
    UUID key = UUID.randomUUID();

    // When
    UploadFileManager.AsyncDownloadResult downloadResult =
        uploadFileManager.downloadDataFile(
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
