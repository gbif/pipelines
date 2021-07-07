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
  public void uploadMultiPartFileTest() {
    try (InputStream archive = readTestFileInputStream("/dwca/Archive.zip")) {
      MockMultipartFile mockMultipartFile =
          new MockMultipartFile("file", "Archive.zip", "application/zip", archive);
      // directory inside the storeDirectory in which file is upload
      UUID targetDirectory = UUID.randomUUID();

      UploadFileManager uploadFileManager =
          new UploadFileManager(
              workingDirectory.toString(),
              storeDirectory.toString(),
              ctx.getBean(DownloadFileManager.class));

      UploadFileManager.AsyncDataFileTask task =
          uploadFileManager.uploadDataFile(mockMultipartFile, targetDirectory.toString());

      // Wait the task to finish
      DataFile dataFile = task.getTask().get();

      assertTrue(Files.exists(dataFile.getFilePath()));
      assertTrue(dataFile.getSize() > 0);
      assertEquals("Archive.zip", dataFile.getSourceFileName());
      assertEquals(FileFormat.DWCA, dataFile.getFileFormat());
      assertEquals("application/zip", dataFile.getMediaType());
      assertEquals("application/zip", dataFile.getReceivedAsMediaType());
    }
  }

  @SneakyThrows
  @Test
  public void downloadFileTest() {
    UploadFileManager uploadFileManager =
        new UploadFileManager(
            workingDirectory.toString(),
            storeDirectory.toString(),
            ctx.getBean(DownloadFileManager.class));
    UUID key = UUID.randomUUID();

    UploadFileManager.AsyncDownloadResult downloadResult =
        uploadFileManager.downloadDataFile(
            testPath("/Archive.zip"),
            key.toString(),
            file -> assertTrue(Files.exists(file.getFilePath())),
            err -> fail());
    File downloadFile = downloadResult.getDownloadTask().get();
    assertTrue(Files.exists(downloadFile.toPath()));
    assertTrue(downloadResult.getDataFile().getSize() > 0);
    assertEquals("Archive.zip", downloadResult.getDataFile().getSourceFileName());
  }
}
