package org.gbif.validator.ws.file;

import static org.gbif.validator.ws.file.DownloadFileManager.isAvailable;
import static org.gbif.validator.ws.file.MediaTypeAndFormatDetector.detectMediaType;
import static org.gbif.validator.ws.file.MediaTypeAndFormatDetector.evaluateMediaTypeAndFormat;
import static org.gbif.validator.ws.file.SupportedMediaTypes.COMPRESS_CONTENT_TYPE;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;
import lombok.Builder;
import lombok.Data;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.FileUtils;
import org.gbif.utils.file.CompressionUtil;
import org.springframework.web.multipart.MultipartFile;

/**
 * Class responsible to manage files uploaded for validation. This class will unzip the file is
 * required.
 */
@Slf4j
public class FileStoreManager {

  @Data
  @Builder
  public static class AsyncDownloadResult {
    private final DataFile dataFile;
    private final CompletableFuture<File> downloadTask;
  }

  @Data
  @Builder
  public static class AsyncDataFileTask {
    private final DataFile start;
    private final CompletableFuture<DataFile> task;
  }

  private final Path storePath;

  private final DownloadFileManager downloadFileManager;

  public FileStoreManager(
      String workingDirectory, String storeDirectory, DownloadFileManager downloadFileManager)
      throws IOException {
    Path workingDirectory1 = Paths.get(workingDirectory);
    this.storePath = Paths.get(storeDirectory);
    this.downloadFileManager = downloadFileManager;
    createIfNotExists(workingDirectory1);
    createIfNotExists(storePath);
  }

  public CompletableFuture<DataFile> extractAndGetFileInfoAsync(
      Path dataFilePath, Path destinationFolder, String fileName) {
    return CompletableFuture.supplyAsync(
        () -> extractAndGetFileInfo(dataFilePath, destinationFolder, fileName));
  }

  @SneakyThrows
  public DataFile extractAndGetFileInfo(
      Path dataFilePath, Path destinationFolder, String fileName) {
    try {

      // check if we have something to unzip
      String detectedMediaType = detectMediaType(dataFilePath);
      if (COMPRESS_CONTENT_TYPE.contains(detectedMediaType)) {
        CompressionUtil.decompressFile(destinationFolder.toFile(), dataFilePath.toFile());
      }

      // from here we can decide to change the content type (e.g. zipped excel file)
      return fromMediaTypeAndFormat(dataFilePath, fileName, detectedMediaType, destinationFolder);
    } catch (Exception ex) {
      log.warn("Deleting temporary content of {} after IOException.", fileName);
      FileUtils.deleteDirectory(destinationFolder.toFile());
      throw ex;
    }
  }

  private DataFile fromMediaTypeAndFormat(
      Path dataFilePath, String fileName, String detectedMediaType, Path finalPath)
      throws UnsupportedMediaTypeException, IOException {
    return evaluateMediaTypeAndFormat(finalPath, detectedMediaType)
        .map(
            mtf ->
                DataFile.create(
                    dataFilePath,
                    fileName,
                    mtf.getFileFormat(),
                    detectedMediaType,
                    mtf.getMediaType()))
        .orElseThrow(
            () -> new UnsupportedMediaTypeException("Unsupported file type: " + detectedMediaType));
  }

  @SneakyThrows
  public AsyncDataFileTask uploadDataFile(MultipartFile multipartFile, String targetDirectory) {
    String fileName = multipartFile.getOriginalFilename();
    Path destinationFolder = getDestinationPath(targetDirectory);
    Path dataFilePath = destinationFolder.resolve(fileName);

    createIfNotExists(destinationFolder);

    // copy the file
    multipartFile.transferTo(dataFilePath.toFile());

    // check if we have something to unzip
    return AsyncDataFileTask.builder()
        .start(DataFile.builder().sourceFileName(fileName).filePath(dataFilePath).build())
        .task(extractAndGetFileInfoAsync(dataFilePath, destinationFolder, fileName))
        .build();
  }

  @SneakyThrows
  public AsyncDownloadResult downloadDataFile(
      String url,
      String targetDirectory,
      Consumer<DataFile> resultCallback,
      Consumer<Throwable> errorCallback)
      throws IOException {

    if (!isAvailable(url)) {
      throw new IllegalArgumentException(
          "Failed to download file from "
              + url
              + ". The resource is not reachable. Please check that the URL is correct");
    }

    String fileName = getFileName(url);
    Path destinationFolder = getDestinationPath(targetDirectory);
    createIfNotExists(destinationFolder);
    Path dataFilePath = getDestinationPath(targetDirectory).resolve(fileName);
    return AsyncDownloadResult.builder()
        .dataFile(DataFile.builder().sourceFileName(fileName).filePath(dataFilePath).build())
        .downloadTask(
            downloadFileManager.downloadAsync(
                url,
                dataFilePath,
                file ->
                    resultCallback.accept(
                        extractAndGetFileInfo(dataFilePath, destinationFolder, fileName)),
                errorCallback))
        .build();
  }

  @SneakyThrows
  public void deleteIfExist(String path) {
    File file = getDestinationPath(path).toFile();
    if (file.exists()) {
      FileUtils.deleteDirectory(file);
    }
  }

  @SneakyThrows
  private static String getFileName(String url) {
    return Paths.get(new URI(url).getPath()).getFileName().toString();
  }

  /** Creates the directory(path) if it doesn't exist. */
  private static void createIfNotExists(Path path) throws IOException {
    if (!path.toFile().exists()) {
      Files.createDirectories(path);
    }
  }

  /** Creates a new random path to be used when copying files. */
  private Path getDestinationPath(String destinationDirectory) {
    return storePath.resolve(destinationDirectory);
  }
}
