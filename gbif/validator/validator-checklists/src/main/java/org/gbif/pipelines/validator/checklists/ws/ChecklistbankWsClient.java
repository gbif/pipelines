package org.gbif.pipelines.validator.checklists.ws;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import lombok.Data;
import org.gbif.dwc.terms.Term;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;

/** Ws Client for the checklist validation using the checklistbank.org validator. */
@RequestMapping(produces = MediaType.APPLICATION_JSON_VALUE)
public interface ChecklistbankWsClient {

  @PostMapping(
      value = "validator",
      consumes = {"application/octet-stream", "application/zip"})
  ValidatorResponse validateArchive(byte[] file);

  @Data
  @JsonIgnoreProperties(ignoreUnknown = true)
  class ValidatorResponse {
    private int key;
  }

  @GetMapping(path = "importer/{key}")
  ImporterResponse checkImporter(@PathVariable("key") int key);

  @Data
  @JsonIgnoreProperties(ignoreUnknown = true)
  class ImporterResponse {
    private int datasetKey;
    private int attempt;
    private String state;
    private Long bareNameCount;
    private Long distributionCount;
    private Long estimateCount;
    private Long mediaCount;
    private Long nameCount;
    private Long referenceCount;
    private Long synonymCount;
    private Long taxonCount;
    private Long treatmentCount;
    private Long typeMaterialCount;
    private Long vernacularCount;
    private Long usagesCount;
    private Map<String, Long> issuesCount = new HashMap<>();
    private Map<Term, Long> verbatimByTermCount = new HashMap<>();
    private Map<Term, Map<Term, Long>> verbatimByRowTypeCount = new HashMap<>();
  }

  @GetMapping(path = "dataset/{key}/verbatim")
  VerbatimResponse getVerbatim(
      @PathVariable("key") int key,
      @RequestParam(value = "type", required = false) String type,
      @RequestParam(value = "issue", required = false) String issue,
      @RequestParam(value = "limit", required = false) int limit);

  @Data
  @JsonIgnoreProperties(ignoreUnknown = true)
  class VerbatimResponse {
    private int offset;
    private int limit;
    private int total;
    private List<VerbatimResult> result;
  }

  @Data
  @JsonIgnoreProperties(ignoreUnknown = true)
  class VerbatimResult {
    private long id;
    private int datasetKey;
    private String file;
    private String type;
    private Map<Term, String> terms;
    private List<String> issues;
  }
}
