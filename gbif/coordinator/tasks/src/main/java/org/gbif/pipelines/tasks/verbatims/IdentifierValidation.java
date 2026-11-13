package org.gbif.pipelines.tasks.verbatims;

import com.google.common.base.Strings;
import java.io.IOException;
import java.io.InputStream;
import java.util.Iterator;
import java.util.Optional;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.file.DataFileStream;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.gbif.api.model.crawler.OccurrenceValidationReport;
import org.gbif.converters.parser.xml.parsing.validators.UniquenessValidator;
import org.gbif.dwc.terms.DwcTerm;
import org.gbif.pipelines.common.utils.HdfsUtils;
import org.gbif.pipelines.core.pojo.HdfsConfigs;
import org.gbif.pipelines.core.utils.FsUtils;
import org.gbif.pipelines.core.utils.ModelUtils;
import org.gbif.pipelines.io.avro.ExtendedRecord;

/**
 * Validates the occurrenceIDs and triplets (institutionCode, collectionCode, catalogNumber) of
 * verbatim records, producing the same {@link OccurrenceValidationReport} the crawler produces for
 * DwC archives, and decides from it whether the triplets and occurrenceIDs can be used to look up
 * and assign GBIF ids.
 *
 * <p>For sources without a crawler report (e.g. XML) the report is calculated from the verbatim
 * avro and stored in the step's meta file, so that it can be read again when the outgoing message
 * is created.
 */
@Slf4j
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class IdentifierValidation {

  // meta file keys, none is a prefix of another as HdfsUtils#getValueByKey matches by prefix
  public static final String CHECKED_RECORDS = "idValidationCheckedRecords";
  public static final String UNIQUE_TRIPLETS = "idValidationUniqueTriplets";
  public static final String RECORDS_WITH_INVALID_TRIPLETS =
      "idValidationRecordsWithInvalidTriplets";
  public static final String UNIQUE_OCCURRENCE_IDS = "idValidationUniqueOccurrenceIds";
  public static final String RECORDS_MISSING_OCCURRENCE_ID =
      "idValidationRecordsMissingOccurrenceId";

  /**
   * Triplets are valid if there are more than 0 unique triplets in the dataset, and exactly 0
   * triplets referenced by more than one record. Records with an incomplete triplet are ignored.
   * Without a report the triplets are assumed to be valid.
   */
  public static boolean tripletsValid(OccurrenceValidationReport report) {
    if (report == null) {
      return true;
    }
    return report.getUniqueTriplets() > 0
        && report.getCheckedRecords() - report.getRecordsWithInvalidTriplets()
            == report.getUniqueTriplets();
  }

  /**
   * OccurrenceIds are valid if each record has a unique occurrenceId. Without a report the
   * occurrenceIds are assumed to be valid.
   */
  public static boolean occurrenceIdsValid(OccurrenceValidationReport report) {
    if (report == null) {
      return true;
    }
    return report.getCheckedRecords() > 0
        && report.getUniqueOccurrenceIds() == report.getCheckedRecords();
  }

  /** Counts the unique, missing and incomplete identifiers of the records. */
  public static OccurrenceValidationReport validate(Iterator<ExtendedRecord> records) {
    long checked = 0;
    long uniqueTriplets = 0;
    long invalidTriplets = 0;
    long uniqueOccurrenceIds = 0;
    long missingOccurrenceIds = 0;

    try (UniquenessValidator triplets = UniquenessValidator.getNewInstance();
        UniquenessValidator occurrenceIds = UniquenessValidator.getNewInstance()) {
      while (records.hasNext()) {
        ExtendedRecord er = records.next();
        checked++;

        // same rules as the keygen, see OccurrenceKeyBuilder#buildKey and GbifIdInterpreter
        String ic = ModelUtils.extractValue(er, DwcTerm.institutionCode);
        String cc = ModelUtils.extractValue(er, DwcTerm.collectionCode);
        String cn = ModelUtils.extractValue(er, DwcTerm.catalogNumber);
        if (Strings.isNullOrEmpty(ic) || Strings.isNullOrEmpty(cc) || Strings.isNullOrEmpty(cn)) {
          invalidTriplets++;
        } else if (triplets.isUnique(ic + "|" + cc + "|" + cn)) {
          uniqueTriplets++;
        }

        String occurrenceId = ModelUtils.extractValue(er, DwcTerm.occurrenceID);
        if (Strings.isNullOrEmpty(occurrenceId)) {
          missingOccurrenceIds++;
        } else if (occurrenceIds.isUnique(occurrenceId)) {
          uniqueOccurrenceIds++;
        }
      }
    }

    return new OccurrenceValidationReport(
        Math.toIntExact(checked),
        Math.toIntExact(uniqueTriplets),
        Math.toIntExact(invalidTriplets),
        Math.toIntExact(uniqueOccurrenceIds),
        Math.toIntExact(missingOccurrenceIds),
        true);
  }

  /** Validates the records of a verbatim avro file. */
  public static OccurrenceValidationReport validate(HdfsConfigs hdfsConfigs, String avroPath)
      throws IOException {
    FileSystem fs = FsUtils.getFileSystem(hdfsConfigs, avroPath);
    try (InputStream in = fs.open(new Path(avroPath));
        DataFileStream<ExtendedRecord> records =
            new DataFileStream<>(in, new SpecificDatumReader<>(ExtendedRecord.class))) {
      return validate(records.iterator());
    }
  }

  /** Appends the report counts to a yaml meta file, creating it if it doesn't exist. */
  public static void appendToMetaFile(
      HdfsConfigs hdfsConfigs, String metaPath, OccurrenceValidationReport report)
      throws IOException {
    FileSystem fs = FsUtils.getFileSystem(hdfsConfigs, metaPath);
    Path path = new Path(metaPath);

    StringBuilder body = new StringBuilder();
    if (fs.exists(path)) {
      try (InputStream in = fs.open(path)) {
        // meta files are written with writeChars (UTF-16), drop the high bytes as the readers do
        body.append(
            new String(in.readAllBytes(), java.nio.charset.StandardCharsets.UTF_8)
                .replace("\u0000", ""));
      }
      if (body.length() > 0 && body.charAt(body.length() - 1) != '\n') {
        body.append('\n');
      }
    }
    body.append(CHECKED_RECORDS)
        .append(": ")
        .append(report.getCheckedRecords())
        .append('\n')
        .append(UNIQUE_TRIPLETS)
        .append(": ")
        .append(report.getUniqueTriplets())
        .append('\n')
        .append(RECORDS_WITH_INVALID_TRIPLETS)
        .append(": ")
        .append(report.getRecordsWithInvalidTriplets())
        .append('\n')
        .append(UNIQUE_OCCURRENCE_IDS)
        .append(": ")
        .append(report.getUniqueOccurrenceIds())
        .append('\n')
        .append(RECORDS_MISSING_OCCURRENCE_ID)
        .append(": ")
        .append(report.getRecordsMissingOccurrenceId())
        .append('\n');

    FsUtils.createFile(fs, path, body.toString());
  }

  /**
   * Reads a report written by {@link #appendToMetaFile}.
   *
   * @return empty if the meta file doesn't contain a report, e.g. it was written before the
   *     validation existed
   */
  public static Optional<OccurrenceValidationReport> readFromMetaFile(
      HdfsConfigs hdfsConfigs, String metaPath) throws IOException {
    Optional<Long> checked = HdfsUtils.getLongByKey(hdfsConfigs, metaPath, CHECKED_RECORDS);
    if (checked.isEmpty()) {
      return Optional.empty();
    }
    return Optional.of(
        new OccurrenceValidationReport(
            Math.toIntExact(checked.get()),
            readInt(hdfsConfigs, metaPath, UNIQUE_TRIPLETS),
            readInt(hdfsConfigs, metaPath, RECORDS_WITH_INVALID_TRIPLETS),
            readInt(hdfsConfigs, metaPath, UNIQUE_OCCURRENCE_IDS),
            readInt(hdfsConfigs, metaPath, RECORDS_MISSING_OCCURRENCE_ID),
            true));
  }

  private static int readInt(HdfsConfigs hdfsConfigs, String metaPath, String key)
      throws IOException {
    return Math.toIntExact(
        HdfsUtils.getLongByKey(hdfsConfigs, metaPath, key)
            .orElseThrow(() -> new IOException("Missing " + key + " in " + metaPath)));
  }
}
