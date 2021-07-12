package org.gbif.validator.ws.file;

import java.nio.file.Path;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;
import lombok.SneakyThrows;
import lombok.experimental.UtilityClass;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.FilenameUtils;
import org.gbif.utils.file.CompressionUtil;

@Slf4j
@UtilityClass
public class CompressUtil {

  protected static CompletableFuture<Path> decompressAsync(
      Path zipFile,
      Path targetLocation,
      Consumer<Path> successCallback,
      Consumer<Throwable> errorCallback) {
    return CompletableFuture.supplyAsync(() -> decompress(zipFile, targetLocation))
        .whenComplete(
            (result, error) -> {
              if (error != null) {
                log.error("Error extracting file  " + zipFile, error);
                errorCallback.accept(error);
              } else {
                successCallback.accept(result);
              }
            });
  }

  @SneakyThrows
  protected static Path decompress(Path compressedFile, Path targetLocation) {
    Path extractPath =
        targetLocation.resolve(FilenameUtils.getBaseName(compressedFile.toFile().getName()));
    CompressionUtil.decompressFile(extractPath.toFile(), compressedFile.toFile());
    return extractPath;
  }
}
