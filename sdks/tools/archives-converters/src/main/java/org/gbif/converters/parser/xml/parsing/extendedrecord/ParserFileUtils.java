package org.gbif.converters.parser.xml.parsing.extendedrecord;

import java.io.File;
import java.nio.file.Files;
import java.util.function.BinaryOperator;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.gbif.converters.parser.xml.ParsingException;

@Slf4j
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class ParserFileUtils {

  private static final BinaryOperator<String> UNCOMPRESS =
      (inPath, outPath) -> String.format("tar -xf %s -C %s", inPath, outPath);
  private static final String ARCHIVE_PREFIX = ".tar.xz";
  private static final String TMP_PATH = "/tmp/";

  /**
   * @param inputPath path to a folder with xmls or a tar.xz archive
   * @return the new File object, path to xml files folder
   */
  public static File uncompressAndGetInputFile(String inputPath) {
    // Check directory
    File inputFile = new File(inputPath);
    if (!inputFile.exists()) {
      throw new ParsingException(
          "Directory or file " + inputFile.getAbsolutePath() + " does not exist");
    }

    // Uncompress if it is a tar.xz
    if (inputFile.isFile()) {
      if (!inputFile.getPath().endsWith(ARCHIVE_PREFIX)) {
        throw new ParsingException("Wrong archive extension -" + inputFile.getAbsolutePath());
      }
      return uncompress(inputFile);
    }

    return inputFile;
  }

  /**
   * Uncompress a tar.xz archive
   *
   * @param inputFile - *.tar.xz file
   * @return the new File object, path to uncompressed files folder
   */
  @SneakyThrows
  public static File uncompress(File inputFile) {
    File parentFile = new File(inputFile.getParentFile(), TMP_PATH);
    Files.createDirectories(parentFile.toPath());
    String cmd = UNCOMPRESS.apply(inputFile.getAbsolutePath(), parentFile.getAbsolutePath());

    log.info("Uncompressing a tar.xz archive {}", cmd);
    Runtime.getRuntime().exec(cmd).waitFor();
    log.info("The archive has been uncompressed");

    return parentFile;
  }
}
