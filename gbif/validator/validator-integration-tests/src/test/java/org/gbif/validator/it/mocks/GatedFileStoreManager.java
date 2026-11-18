package org.gbif.validator.it.mocks;

import java.io.IOException;
import java.nio.file.Path;
import java.util.concurrent.CompletableFuture;
import org.gbif.validator.ws.file.DataFile;
import org.gbif.validator.ws.file.DownloadFileManager;
import org.gbif.validator.ws.file.FileStoreManager;

/** FileStoreManager whose file extraction can be held, so tests control when submissions end. */
public class GatedFileStoreManager extends FileStoreManager {

  private volatile CompletableFuture<Void> gate = CompletableFuture.completedFuture(null);

  public GatedFileStoreManager(
      String workingDirectory, String storeDirectory, DownloadFileManager downloadFileManager)
      throws IOException {
    super(workingDirectory, storeDirectory, downloadFileManager);
  }

  /** Holds the extraction of files uploaded from now on, until the returned gate is completed. */
  public CompletableFuture<Void> hold() {
    gate = new CompletableFuture<>();
    return gate;
  }

  @Override
  public CompletableFuture<DataFile> extractAndGetFileInfoAsync(
      Path dataFilePath, Path destinationFolder, String fileName) {
    return gate.thenApplyAsync(
        v -> extractAndGetFileInfo(dataFilePath, destinationFolder, fileName));
  }
}
