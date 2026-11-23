package org.gbif.pipelines.tasks.validators.validator.validate;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.stream.Stream;

/**
 * The file a user uploaded to the validator, in its folder of the validation store ({@code
 * <archiveRepository>/<validationKey>/}). The validation web service unpacks it next to itself, so
 * the folder holds the upload and its content. In orchestrator mode the validator doesn't fetch the
 * validation (where validator-ws kept the file name): the upload is the one top-level file that is
 * a zip or gzip archive.
 */
final class UploadedArchive {

  private UploadedArchive() {}

  static Path find(Path validationFolder) throws IOException {
    List<Path> archives;
    try (Stream<Path> files = Files.list(validationFolder)) {
      archives = files.filter(Files::isRegularFile).filter(UploadedArchive::isArchive).toList();
    }
    if (archives.size() != 1) {
      throw new IllegalStateException(
          "Expected one uploaded archive in "
              + validationFolder
              + ", found "
              + archives.stream().map(p -> p.getFileName().toString()).toList());
    }
    return archives.get(0);
  }

  /** Zip ({@code PK\3\4}) or gzip ({@code 1F 8B}) by its first bytes. */
  static boolean isArchive(Path file) {
    try (InputStream in = Files.newInputStream(file)) {
      byte[] magic = in.readNBytes(4);
      boolean zip =
          magic.length == 4 && magic[0] == 'P' && magic[1] == 'K' && magic[2] == 3 && magic[3] == 4;
      boolean gzip = magic.length >= 2 && (magic[0] & 0xFF) == 0x1F && (magic[1] & 0xFF) == 0x8B;
      return zip || gzip;
    } catch (IOException e) {
      return false;
    }
  }
}
