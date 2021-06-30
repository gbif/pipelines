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

import com.google.common.base.Strings;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Sets;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nullable;
import org.gbif.api.model.crawler.DwcaValidationReport;
import org.gbif.api.model.crawler.GenericValidationReport;
import org.gbif.api.model.crawler.OccurrenceValidationReport;
import org.gbif.api.model.registry.Dataset;
import org.gbif.api.vocabulary.DatasetType;
import org.gbif.dwc.Archive;
import org.gbif.dwc.record.Record;
import org.gbif.dwc.record.StarRecord;
import org.gbif.dwc.terms.DwcTerm;
import org.gbif.dwc.terms.Term;
import org.gbif.utils.file.ClosableIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
public class DwcaValidator {

  private static final Logger LOG = LoggerFactory.getLogger(DwcaValidator.class);

  // The mapping of dataset type to primary key term in the core
  private static final Map<DatasetType, DwcTerm> DATASET_TYPE_CORE_ID =
      ImmutableMap.of(
          DatasetType.CHECKLIST, DwcTerm.taxonID,
          DatasetType.SAMPLING_EVENT, DwcTerm.eventID);

  // limit the number of checked records to protect against memory exhaustion
  private static final int MAX_RECORDS = 2000000;

  // the number of samples to store to illustrate issues
  private static final int MAX_EXAMPLE_ERRORS = 100;

  private int checkedRecords;
  private int recordsWithInvalidTriplets;
  private int recordsMissingOccurrenceId;
  // unique occurrenceIds
  private Set<String> uniqueOccurrenceIds = Sets.newHashSet();
  // unique triplets
  private Set<String> uniqueTriplets = Sets.newHashSet();

  private DwcaValidator() {}

  /**
   * Produce a report with the counts of good and bad unique identifiers (triplets and occurrenceId)
   * in the archive.
   *
   * @param dataset the parent Dataset of the archive
   * @param archive the archive as opened by the dwca-io project's {@link org.gbif.dwc.DwcFiles}
   * @return a report with the counts of good, bad and missing identifiers
   */
  public static DwcaValidationReport validate(Dataset dataset, Archive archive) throws IOException {
    DwcaValidator validator = new DwcaValidator();
    return validator.check(dataset, archive);
  }

  /**
   * Produce a report for a metadata-only dataset.
   *
   * @param dataset the parent Dataset of the metadata
   * @param metadata the metadata file contents
   * @return a "passed" report
   */
  public static DwcaValidationReport validate(Dataset dataset, String metadata) throws IOException {
    return new DwcaValidationReport(
        dataset.getKey(),
        new GenericValidationReport(0, true, Collections.emptyList(), Collections.emptyList()));
  }

  /**
   * Internal non static working method that does the validation. If an occurrence core is found
   * this is what gets validated. Otherwise extension records from either dwc:Occurrence or
   * gbif:TypesAndSpecimen are validated with Occurrence being the preferred extension.
   */
  private DwcaValidationReport check(Dataset dataset, Archive archive) throws IOException {
    if (dataset.getType() == DatasetType.OCCURRENCE) {
      return new DwcaValidationReport(dataset.getKey(), validateOccurrenceCore(archive));

    } else if (DATASET_TYPE_CORE_ID.keySet().contains(dataset.getType())) {
      GenericValidationReport report =
          validateGenericCore(archive, DATASET_TYPE_CORE_ID.get(dataset.getType()));

      // validate any occurrence extension
      if (archive.getExtension(DwcTerm.Occurrence) == null) {
        LOG.info(
            "Dataset [{}] of type[{}] has an archive with no mapped occurrence extension",
            dataset.getKey(),
            dataset.getType());
        return new DwcaValidationReport(dataset.getKey(), report);
      } else {
        return new DwcaValidationReport(
            dataset.getKey(),
            validateOccurrenceExtension(archive, DwcTerm.Occurrence),
            report,
            null);
      }
    } else if (dataset.getType() == DatasetType.METADATA) {
      // TODO validate the EML (requires the new validator library)
      return new DwcaValidationReport(
          dataset.getKey(),
          new GenericValidationReport(0, true, Collections.emptyList(), Collections.emptyList()));
    } else {
      LOG.info(
          "DwC-A for dataset[{}] of type[{}] is INVALID because it is not a supported type",
          dataset.getKey(),
          dataset.getType());
      return new DwcaValidationReport(
          dataset.getKey(), "Dataset type[" + dataset.getType() + "] is not supported in indexing");
    }
  }

  /**
   * Performs basic checking that the primary key constraints are satisfied (present and unique).
   *
   * @param archive To check
   * @param term To use to verify the uniqueness (e.g. DwcTerm.taxonID for taxon core)
   * @return The report produced
   */
  private GenericValidationReport validateGenericCore(Archive archive, Term term)
      throws IOException {
    int records = 0;
    List<String> duplicateIds = new ArrayList<>();
    List<Integer> linesMissingIds = new ArrayList<>();
    Set<String> ids = Sets.newHashSet();
    final boolean useCoreID = !archive.getCore().hasTerm(term);

    try (ClosableIterator<Record> it = archive.getCore().iterator(true, true)) {
      while (it.hasNext()) {
        Record rec = it.next();
        records++;
        String id = useCoreID ? rec.id() : rec.value(term);
        if (linesMissingIds.size() < MAX_EXAMPLE_ERRORS && Strings.isNullOrEmpty(id)) {
          linesMissingIds.add(records);
        }
        if (duplicateIds.size() < MAX_EXAMPLE_ERRORS && ids.contains(id)) {
          duplicateIds.add(id);
        }

        if (!Strings.isNullOrEmpty(id) && ids.size() < MAX_RECORDS) {
          ids.add(id);
        }
      }
    } catch (Exception e) {
      throw new IOException(e);
    }
    return new GenericValidationReport(
        records, records != MAX_RECORDS, duplicateIds, linesMissingIds);
  }

  private OccurrenceValidationReport validateOccurrenceCore(Archive archive) throws IOException {
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
        checkedRecords != MAX_RECORDS);
  }

  private OccurrenceValidationReport validateOccurrenceExtension(
      Archive archive, final Term rowType) throws IOException {

    // this can take some time if the archive includes extension(s)
    archive.initialize();

    // outer loop over core records, e.g. taxa or samples
    try (ClosableIterator<StarRecord> iterator = archive.iterator()) {
      while (iterator.hasNext()) {
        StarRecord star = iterator.next();
        // inner loop over extension records
        List<Record> records = star.extension(rowType);
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
        checkedRecords != MAX_RECORDS);
  }

  /**
   * @param rec
   * @param triplet
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

    return checkedRecords == MAX_RECORDS;
  }

  /**
   * Creates a triplet string if pieces are found in either the core or the occurrence extension.
   *
   * @return the triplet string or null if it cant be found
   */
  private String getTriplet(Record core, Record ext) {
    String institutionCode = valueFromExtOverCore(core, ext, DwcTerm.institutionCode);
    String collectionCode = valueFromExtOverCore(core, ext, DwcTerm.collectionCode);
    String catalogNumber = valueFromExtOverCore(core, ext, DwcTerm.catalogNumber);

    if (!Strings.isNullOrEmpty(institutionCode)
        && !Strings.isNullOrEmpty(collectionCode)
        && !Strings.isNullOrEmpty(catalogNumber)) {
      return institutionCode + "§" + collectionCode + "§" + catalogNumber;
    }
    return null;
  }

  private String valueFromExtOverCore(Record core, @Nullable Record ext, Term term) {
    if (ext != null && !Strings.isNullOrEmpty(ext.value(term))) {
      return ext.value(term);
    }
    return core.value(term);
  }
}
