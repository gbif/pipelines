package org.gbif.pipelines.ingest.utils;

import java.io.FileNotFoundException;
import java.io.InputStream;

import org.gbif.pipelines.parsers.config.model.PipelinesConfig;
import org.gbif.pipelines.parsers.config.model.VocabularyConfig;
import org.gbif.pipelines.transforms.SerializableSupplier;
import org.gbif.vocabulary.lookup.VocabularyLookup;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;

import static java.util.Objects.requireNonNull;

import static org.gbif.pipelines.ingest.utils.FsUtils.buildPath;
import static org.gbif.pipelines.ingest.utils.FsUtils.getFileSystem;

/**
 * Factory to create instances of {@link VocabularyLookup} from a file containing an exported
 * vocabulary.
 */
@Slf4j
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class FileVocabularyFactory {

  public static SerializableSupplier<VocabularyLookup> getInstanceSupplier(
      PipelinesConfig config, String hdfsSiteConfig, VocabularyBackedTerm vocabularyBackedTerm) {
    return () -> {
      VocabularyConfig vocabularyConfig = requireNonNull(config.getVocabularyConfig());

      if (vocabularyBackedTerm == VocabularyBackedTerm.LIFE_STAGE) {
        return VocabularyLookup.load(
            readVocabularyFile(
                hdfsSiteConfig,
                vocabularyConfig.getVocabulariesPath(),
                vocabularyConfig.getLifeStageVocabName()));
      }

      throw new IllegalArgumentException(
          "Vocabulary-backed term not supported: " + vocabularyBackedTerm);
    };
  }

  /**
   * Reads a vocabulary file from HDFS/Local FS
   *
   * @param hdfsSiteConfig HDFS config file
   * @param vocabulariesDir dir where the vocabulary files are
   * @param vocabularyName name of the vocabulary. It has to be the same as the one used in the file
   *     name.
   * @return {@link InputStream}
   */
  @SneakyThrows
  private static InputStream readVocabularyFile(
      String hdfsSiteConfig, String vocabulariesDir, String vocabularyName) {
    FileSystem fs = getFileSystem(hdfsSiteConfig, vocabulariesDir);
    Path fPath = buildPath(vocabulariesDir, vocabularyName + ".json");
    if (fs.exists(fPath)) {
      log.info("Reading vocabularies path - {}", fPath);
      return fs.open(fPath);
    }

    throw new FileNotFoundException("The vocabulary file doesn't exist - " + fPath);
  }

  /** Enum with the terms that are backed by a vocabulary. */
  public enum VocabularyBackedTerm {
    LIFE_STAGE;
  }
}
