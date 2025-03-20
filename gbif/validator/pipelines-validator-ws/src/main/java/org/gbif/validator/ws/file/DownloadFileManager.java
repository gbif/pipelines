package org.gbif.validator.ws.file;

import java.io.File;
import java.io.FileOutputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Service;

@Slf4j
@Service
public class DownloadFileManager {

  private static final Set<String> SUPPORTED_CONTENT_TYPES =
      Set.of("application/zip", "application/xml", "text/csv");

  public static boolean isAvailable(String url) {
    try {
      HttpURLConnection con = (HttpURLConnection) new URL(url).openConnection();
      con.setRequestMethod("HEAD");

      int responseCode = con.getResponseCode();
      String content = con.getHeaderField("Content-Type");

      return responseCode == HttpURLConnection.HTTP_OK
          && SUPPORTED_CONTENT_TYPES.stream().anyMatch(content::contains);
    } catch (Exception e) {
      log.warn("Error getting file information", e);
      return false;
    }
  }

  @SneakyThrows
  public File download(String url, Path targetFilePath) {
    Files.createDirectories(targetFilePath.getParent());
    File targetFile = targetFilePath.toFile();
    try (ReadableByteChannel in = Channels.newChannel(new URL(url).openStream());
        FileOutputStream fileOutputStream = new FileOutputStream(targetFile)) {
      fileOutputStream.getChannel().transferFrom(in, 0, Long.MAX_VALUE);
      return targetFile;
    }
  }

  @Async
  @SneakyThrows
  public CompletableFuture<File> downloadAsync(
      String url,
      Path targetFilePath,
      Consumer<File> successCallback,
      Consumer<Throwable> errorCallback) {
    return CompletableFuture.supplyAsync(() -> download(url, targetFilePath))
        .whenComplete(
            (result, error) -> {
              if (error != null) {
                log.error("Error downloading file from url " + url, error);
                errorCallback.accept(error);
              } else {
                successCallback.accept(result);
              }
            });
  }
}
