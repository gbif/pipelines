package org.gbif.pipelines.validator.checklists.ws;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.List;
import java.util.Map;
import lombok.SneakyThrows;
import org.gbif.dwc.terms.DwcTerm;
import org.gbif.dwc.terms.Term;

public class ChecklistbankWsClientMock implements ChecklistbankWsClient {

  public static final int DEFAULT_KEY = 1;
  public static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  private final ImporterResponse importerResponse;

  @SneakyThrows
  public ChecklistbankWsClientMock(String apiImporterResponsePath) {
    importerResponse =
        OBJECT_MAPPER.readValue(
            ClassLoader.getSystemResourceAsStream(apiImporterResponsePath), ImporterResponse.class);
  }

  @Override
  public ValidatorResponse validateArchive(byte[] file) {
    ValidatorResponse validatorResponse = new ValidatorResponse();
    validatorResponse.setKey(DEFAULT_KEY);
    return validatorResponse;
  }

  @Override
  public ImporterResponse checkImporter(int key) {
    return importerResponse;
  }

  @SneakyThrows
  @Override
  public VerbatimResponse getVerbatim(int key, String type, String issue, int limit) {
    VerbatimResponse verbatimResponse =
        OBJECT_MAPPER.readValue(
            ClassLoader.getSystemResourceAsStream("checklists/api_response_verbatim.json"),
            VerbatimResponse.class);

    String file = null;
    if (type.contains("Identifier")) {
      file = "identifier.txt";
    } else if (type.contains("MeasurementOrFact")) {
      file = "measurementorfacts.txt";
    } else if (type.contains("Distribution")) {
      file = "distribution.txt";
    } else if (type.contains("Taxon")) {
      file = "taxon.txt";
    } else if (type.contains("Reference")) {
      file = "reference.txt";
    }

    verbatimResponse.getResult().get(0).setType(type);
    verbatimResponse.getResult().get(0).setFile(file);

    if (issue != null) {
      Map<Term, String> terms = verbatimResponse.getResult().get(0).getTerms();
      terms.put(DwcTerm.kingdom, "k");
      terms.put(DwcTerm.genus, "g");
      terms.put(DwcTerm.phylum, "p");
      terms.put(DwcTerm.order, "o");
      verbatimResponse.getResult().get(0).setIssues(List.of(issue));
      verbatimResponse.getResult().get(0).setTerms(terms);
    }

    return verbatimResponse;
  }
}
