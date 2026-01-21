/*
 * Copyright 2020 Global Biodiversity Information Facility (GBIF)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.gbif.pipelines.validator;

import static org.gbif.validator.api.EvaluationType.OCCURRENCE_NOT_UNIQUELY_IDENTIFIED;
import static org.gbif.validator.api.EvaluationType.RECORD_NOT_UNIQUELY_IDENTIFIED;
import static org.gbif.validator.api.EvaluationType.RECORD_REFERENTIAL_INTEGRITY_VIOLATION;

import jakarta.annotation.Nullable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import lombok.Builder;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.gbif.api.model.crawler.DwcaValidationReport;
import org.gbif.api.model.crawler.GenericValidationReport;
import org.gbif.api.model.crawler.OccurrenceValidationReport;
import org.gbif.api.vocabulary.DatasetType;
import org.gbif.dwc.Archive;
import org.gbif.dwc.record.Record;
import org.gbif.dwc.record.StarRecord;
import org.gbif.dwc.terms.DwcTerm;
import org.gbif.dwc.terms.Term;
import org.gbif.utils.file.ClosableIterator;
import org.gbif.validator.api.Metrics.IssueInfo;

/**
 * This performs the validity checking for DwC-A for the purposes <em>of deciding if the archive is
 * valid to continue in GBIF indexing only</em>. It is not intended to be a validity checker for a
 * wider community who should impose stricter checking than this offers.
 *
 * <p>This verifies:
 *
 * <ul>
 *   <li>Verification that core identifiers are present and unique in the core file
 *   <li>Verification that the contract is respected for uniqueness of either occurrenceID or the
 *       holy triplet (where applicable)
 * </ul>
 *
 * Please note that:
 *
 * <ul>
 *   <li>This class is validating only Occurrence, Taxon and Sample based datasets, marking others
 *       as invalid.
 *   <li>The number of validated records can differ to the number of core records when Occurrence
 *       extensions are used
 * </ul>
 */
@Slf4j
@Builder
public class DwcaValidator {

  // The mapping of dataset type to primary key term in the core
  private static final Map<DatasetType, DwcTerm> DATASET_TYPE_CORE_ID =
      new EnumMap<>(DatasetType.class);

  static {
    DATASET_TYPE_CORE_ID.put(DatasetType.CHECKLIST, DwcTerm.taxonID);
    DATASET_TYPE_CORE_ID.put(DatasetType.SAMPLING_EVENT, DwcTerm.eventID);
  }

  // limit the number of checked records to protect against memory exhaustion
  @Builder.Default private final int maxRecords = 2_000_000;

  // the number of samples to store to illustrate issues
  @Builder.Default private final int maxExampleErrors = 100;

  private final UUID datasetKey;
  private final DatasetType datasetType;
  private final Archive archive;
  private final String metadata;
  private final Term term;

  private int checkedRecords;
  private int recordsWithInvalidTriplets;
  private int recordsMissingOccurrenceId;
  // unique occurrenceIds
  private final Set<String> uniqueOccurrenceIds = new HashSet<>();
  // unique triplets
  private final Set<String> uniqueTriplets = new HashSet<>();

  /**
   * Produce a report with the counts of good and bad unique identifiers (triplets and occurrenceId)
   * in the archive.
   *
   * @return a report with the counts of good, bad and missing identifiers
   */
  @SneakyThrows
  public DwcaValidationReport validateAsReport() {
    if (datasetType == DatasetType.OCCURRENCE) {
      return new DwcaValidationReport(datasetKey, validateOccurrenceCore());
    } else if (DATASET_TYPE_CORE_ID.containsKey(datasetType)) {
      GenericValidationReport report = validateGenericCore();

      // validate any occurrence extension
      if (archive.getExtension(DwcTerm.Occurrence) == null) {
        log.info(
            "Dataset [{}] of type[{}] has an archive with no mapped occurrence extension",
            datasetKey,
            datasetType);
        return new DwcaValidationReport(datasetKey, report);
      } else {
        return new DwcaValidationReport(datasetKey, validateOccurrenceExtension(), report, null);
      }
    } else if (datasetType == DatasetType.METADATA) {
      // TODO validate the EML (requires the new validator library)
      return new DwcaValidationReport(
          datasetKey,
          new GenericValidationReport(0, true, Collections.emptyList(), Collections.emptyList()));
    } else {
      log.info(
          "DwC-A for dataset[{}] of type[{}] is INVALID because it is not a supported type",
          datasetKey,
          datasetType);
      return new DwcaValidationReport(
          datasetKey, "Dataset type[" + datasetType + "] is not supported in indexing");
    }
  }

  public List<IssueInfo> validate() {
    DwcaValidationReport report = validateAsReport();

    List<IssueInfo> issueInfos = new ArrayList<>();

    // Generic report
    GenericValidationReport genericReport = report.getGenericReport();
    if (genericReport != null && !genericReport.isValid()) {
      if (!genericReport.getDuplicateIds().isEmpty()) {
        issueInfos.add(IssueInfo.create(RECORD_NOT_UNIQUELY_IDENTIFIED));
      }
      if (!genericReport.getRowNumbersMissingId().isEmpty()) {
        issueInfos.add(IssueInfo.create(RECORD_REFERENTIAL_INTEGRITY_VIOLATION));
      }
    }

    // Occurrence report
    OccurrenceValidationReport occurrenceReport = report.getOccurrenceReport();
    if (occurrenceReport != null && !occurrenceReport.isValid()) {
      issueInfos.add(IssueInfo.create(OCCURRENCE_NOT_UNIQUELY_IDENTIFIED));
    }

    return issueInfos;
  }

  /**
   * Performs basic checking that the primary key constraints are satisfied (present and unique).
   *
   * @return The report produced
   */
  @SneakyThrows
  private GenericValidationReport validateGenericCore() {
    Term type = DATASET_TYPE_CORE_ID.get(datasetType);
    int records = 0;
    List<String> duplicateIds = new ArrayList<>();
    List<Integer> linesMissingIds = new ArrayList<>();
    Set<String> ids = new HashSet<>();
    final boolean useCoreID = !archive.getCore().hasTerm(type);

    try (ClosableIterator<Record> it = archive.getCore().iterator(true, true)) {
      while (it.hasNext()) {
        Record rec = it.next();
        records++;
        String id = useCoreID ? rec.id() : rec.value(type);
        if (linesMissingIds.size() < maxExampleErrors && isNullOrEmpty(id)) {
          linesMissingIds.add(records);
        }
        if (duplicateIds.size() < maxExampleErrors && ids.contains(id)) {
          duplicateIds.add(id);
        }
        if (!isNullOrEmpty(id) && ids.size() < maxRecords) {
          ids.add(id);
        }
      }
    } catch (Exception ex) {
      throw new IOException(ex);
    }
    return new GenericValidationReport(
        records, records != maxRecords, duplicateIds, linesMissingIds);
  }

  @SneakyThrows
  private OccurrenceValidationReport validateOccurrenceCore() {
    try (ClosableIterator<Record> it = archive.getCore().iterator(true, true)) {
      while (it.hasNext()) {
        Record record = it.next();
        if (checkOccurrenceRecord(record, getTriplet(record, null))) {
          break;
        }
      }
    } catch (Exception e) {
      throw new IOException(e);
    }

    return new OccurrenceValidationReport(
        checkedRecords,
        uniqueTriplets.size(),
        recordsWithInvalidTriplets,
        uniqueOccurrenceIds.size(),
        recordsMissingOccurrenceId,
        checkedRecords != maxRecords);
  }

  @SneakyThrows
  private OccurrenceValidationReport validateOccurrenceExtension() {

    // this can take some time if the archive includes extension(s)
    archive.initialize();

    // outer loop over core records, e.g. taxa or samples
    try (ClosableIterator<StarRecord> iterator = archive.iterator()) {
      while (iterator.hasNext()) {
        StarRecord star = iterator.next();
        // inner loop over extension records
        List<Record> records = star.extension(DwcTerm.Occurrence);
        if (records != null) {
          for (Record ext : records) {
            if (checkOccurrenceRecord(ext, getTriplet(star.core(), ext))) {
              break;
            }
          }
        }
      }
    } catch (Exception e) {
      throw new IOException(e);
    }
    return new OccurrenceValidationReport(
        checkedRecords,
        uniqueTriplets.size(),
        recordsWithInvalidTriplets,
        uniqueOccurrenceIds.size(),
        recordsMissingOccurrenceId,
        checkedRecords != maxRecords);
  }

  /**
   * @return should we continue or not
   */
  private boolean checkOccurrenceRecord(Record rec, String triplet) {
    checkedRecords++;

    // triplet can be part of both, e.g. inst and catalog number could be in the core
    if (triplet == null) {
      recordsWithInvalidTriplets++;
    } else {
      uniqueTriplets.add(triplet);
    }

    // occurrenceId can only be in the extension
    String occurrenceId = rec.value(DwcTerm.occurrenceID);
    if (occurrenceId == null) {
      recordsMissingOccurrenceId++;
    } else {
      uniqueOccurrenceIds.add(occurrenceId);
    }

    return checkedRecords == maxRecords;
  }

  /**
   * Creates a triplet string if pieces are found in either the core or the occurrence extension.
   *
   * @return the triplet string or null if it can't be found
   */
  private String getTriplet(Record core, Record ext) {
    String institutionCode = valueFromExtOverCore(core, ext, DwcTerm.institutionCode);
    String collectionCode = valueFromExtOverCore(core, ext, DwcTerm.collectionCode);
    String catalogNumber = valueFromExtOverCore(core, ext, DwcTerm.catalogNumber);

    if (!isNullOrEmpty(institutionCode)
        && !isNullOrEmpty(collectionCode)
        && !isNullOrEmpty(catalogNumber)) {
      return institutionCode + "§" + collectionCode + "§" + catalogNumber;
    }
    return null;
  }

  private String valueFromExtOverCore(Record core, @Nullable Record ext, Term term) {
    if (ext != null && !isNullOrEmpty(ext.value(term))) {
      return ext.value(term);
    }
    return core.value(term);
  }

  private boolean isNullOrEmpty(String s) {
    return s == null || s.isEmpty();
  }
}
