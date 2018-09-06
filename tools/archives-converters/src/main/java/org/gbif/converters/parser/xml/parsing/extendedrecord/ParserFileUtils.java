package org.gbif.converters.parser.xml.parsing.extendedrecord;

import org.gbif.converters.parser.xml.ParsingException;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.function.BiFunction;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ParserFileUtils {

  private static final Logger LOG = LoggerFactory.getLogger(ParserFileUtils.class);

  private static final BiFunction<String, String, String> UNCOMPRESS =
      (inPath, outPath) -> String.format("tar -xf %s -C %s", inPath, outPath);
  private static final String ARCHIVE_PREFIX = ".tar.xz";
  private static final String TMP_PATH = "/tmp/";

  private ParserFileUtils() {
    // NOP
  }

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
  public static File uncompress(File inputFile) {
    try {
      File parentFile = new File(inputFile.getParentFile(), TMP_PATH);
      Files.createDirectories(parentFile.toPath());
      String cmd = UNCOMPRESS.apply(inputFile.getAbsolutePath(), parentFile.getAbsolutePath());

      LOG.info("Uncompressing a tar.xz archive {}", cmd);
      Runtime.getRuntime().exec(cmd).waitFor();
      LOG.info("The archive has been uncompressed");

      return parentFile;
    } catch (InterruptedException | IOException ex) {
      LOG.error("Directory or file {} does not exist", inputFile.getAbsolutePath());
      throw new ParsingException(ex);
    }
  }
}
