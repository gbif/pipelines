package org.gbif.pipelines.validator.checklist;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.List;
import lombok.SneakyThrows;
import org.gbif.pipelines.validator.serde.ObjectMapperUtils;
import org.gbif.validator.api.ClbDatasetImport;

public class ChecklistbankWsClientMock implements ChecklistbankWsClient {

  public static final int DEFAULT_KEY = 1;
  public static final ObjectMapper OBJECT_MAPPER =
      ObjectMapperUtils.createObjectMapperWithColDPSupport();

  private final String verbatimResponsePath;

  public ChecklistbankWsClientMock() {
    verbatimResponsePath = "checklists/api_response_verbatim.json";
  }

  public ChecklistbankWsClientMock(String verbatimResponsePath) {
    this.verbatimResponsePath = verbatimResponsePath;
  }

  @Override
  public ValidatorResponse validateArchive(String callback, byte[] file) {
    ValidatorResponse validatorResponse = new ValidatorResponse();
    validatorResponse.setKey(DEFAULT_KEY);
    return validatorResponse;
  }

  @Override
  public List<ClbDatasetImport> checkImport(int key) {
    ClbDatasetImport clbDatasetImport = new ClbDatasetImport();
    clbDatasetImport.setDatasetKey(DEFAULT_KEY);
    clbDatasetImport.setStatus(ClbDatasetImport.FINISHED);
    return List.of(clbDatasetImport);
  }

  @SneakyThrows
  @Override
  public VerbatimResponse getVerbatim(int key, String type, String issue, int limit) {
    VerbatimResponse verbatimResponse =
        OBJECT_MAPPER.readValue(
            ClassLoader.getSystemResourceAsStream(verbatimResponsePath), VerbatimResponse.class);

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
    } else {
      file = type + ".txt";
    }

    verbatimResponse.getResult().get(0).setType(type);
    verbatimResponse.getResult().get(0).setFile(file);

    return verbatimResponse;
  }
}
