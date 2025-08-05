package org.gbif.pipelines.validator.checklists.cli;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.Optional;
import java.util.UUID;
import org.gbif.common.messaging.api.MessagePublisher;
import org.gbif.common.messaging.api.messages.PipelinesChecklistValidatorMessage;
import org.gbif.dwc.terms.DwcTerm;
import org.gbif.dwc.terms.Term;
import org.gbif.pipelines.validator.checklists.cli.config.ChecklistValidatorConfiguration;
import org.gbif.validator.api.DwcFileType;
import org.gbif.validator.api.Metrics;
import org.gbif.validator.api.Metrics.FileInfo;
import org.gbif.validator.api.Metrics.TermInfo;
import org.gbif.validator.api.Validation;
import org.gbif.validator.ws.client.ValidationWsClient;

public class ChecklistValidatorCallbackIT {

  //  @Test
  public void checklistTest() {
    // Temp dir
    Path temp = Paths.get(getClass().getResource("/").getFile());

    // State
    ChecklistValidatorConfiguration config = new ChecklistValidatorConfiguration();
    config.archiveRepository = getClass().getResource("/dwca/").getFile();
    config.neoRepository = temp.resolve("neo").toFile();

    ValidationWsClient validationClient = ValidationWsClientStub.create();
    MessagePublisher messagePublisher = MessagePublisherStub.create();

    UUID uuid = UUID.fromString("1e2e9421-6c68-4b78-b988-b6403deeb6dd");

    Validation validation = validationClient.get(uuid);
    validation.setMetrics(
        Metrics.builder()
            .fileInfos(
                Collections.singletonList(
                    FileInfo.builder()
                        .fileName("distribution.txt")
                        .terms(
                            Collections.singletonList(
                                TermInfo.builder()
                                    .term(DwcTerm.locationID.qualifiedName())
                                    .interpretedIndexed(0L)
                                    .rawIndexed(0L)
                                    .build()))
                        .build()))
            .build());

    validationClient.update(validation);

    PipelinesChecklistValidatorMessage message =
        new PipelinesChecklistValidatorMessage(
            uuid, 1, Collections.singleton("VALIDATOR_COLLECT_METRICS"), 1L, "dwca");

    // When
    ChecklistValidatorCallback callback =
        new ChecklistValidatorCallback(config, validationClient, messagePublisher);
    callback.handleMessage(message);

    // Should
    Validation result = validationClient.get(uuid);
    assertEquals(3, result.getMetrics().getFileInfos().size());

    Optional<FileInfo> taxonOpt = getFileInfoByName(result, "taxon.txt");
    assertTrue(taxonOpt.isPresent());
    FileInfo taxon = taxonOpt.get();
    assertEquals(DwcFileType.CORE, taxon.getFileType());
    assertEquals(DwcTerm.Taxon.qualifiedName(), taxon.getRowType());
    assertEquals(Long.valueOf(475L), taxon.getCount());
    assertEquals(Long.valueOf(475L), taxon.getIndexedCount());
    assertEquals(14, taxon.getTerms().size());
    assertEquals(5, taxon.getIssues().size());

    Optional<FileInfo> speciesProfileOpt = getFileInfoByName(result, "speciesprofile.txt");
    assertTrue(speciesProfileOpt.isPresent());
    FileInfo speciesProfile = speciesProfileOpt.get();
    assertEquals(DwcFileType.EXTENSION, speciesProfile.getFileType());
    assertEquals("http://rs.gbif.org/terms/1.0/SpeciesProfile", speciesProfile.getRowType());
    assertEquals(Long.valueOf(475L), speciesProfile.getCount());
    assertEquals(Long.valueOf(475L), speciesProfile.getIndexedCount());
    assertEquals(2, speciesProfile.getTerms().size());
    assertEquals(0, speciesProfile.getIssues().size());
    assertTerm(DwcTerm.habitat, 475, 475, speciesProfile);
    assertTerm("http://rs.gbif.org/terms/1.0/isInvasive", 80, 0, speciesProfile);

    Optional<FileInfo> distributionOpt = getFileInfoByName(result, "distribution.txt");
    assertTrue(distributionOpt.isPresent());
    FileInfo distribution = distributionOpt.get();
    assertEquals(DwcFileType.EXTENSION, distribution.getFileType());
    assertEquals("http://rs.gbif.org/terms/1.0/Distribution", distribution.getRowType());
    assertEquals(Long.valueOf(475L), distribution.getCount());
    assertEquals(Long.valueOf(475L), distribution.getIndexedCount());
    assertEquals(4, distribution.getTerms().size());
    assertEquals(0, distribution.getIssues().size());
    assertTerm(DwcTerm.countryCode, 475, 475, distribution);
    assertTerm(DwcTerm.occurrenceStatus, 475, 475, distribution);
    assertTerm(DwcTerm.establishmentMeans, 475, 0, distribution);
    assertTerm(DwcTerm.locationID, 0, 0, distribution);
    callback.close();
  }

  private Optional<FileInfo> getFileInfoByName(Validation validation, String name) {
    return validation.getMetrics().getFileInfos().stream()
        .filter(x -> x.getFileName().equals(name))
        .findAny();
  }

  private void assertTerm(String term, int rawIndexed, int interpretedIndexed, FileInfo fileInfo) {
    Optional<TermInfo> termInfo =
        fileInfo.getTerms().stream().filter(x -> x.getTerm().equals(term)).findAny();
    assertTrue(termInfo.isPresent());
    TermInfo info = termInfo.get();
    assertEquals(Long.valueOf(rawIndexed), info.getRawIndexed());
    assertEquals(Long.valueOf(interpretedIndexed), info.getInterpretedIndexed());
  }

  private void assertTerm(Term term, int rawIndexed, int interpretedIndexed, FileInfo fileInfo) {
    assertTerm(term.qualifiedName(), rawIndexed, interpretedIndexed, fileInfo);
  }
}
