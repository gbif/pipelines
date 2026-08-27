package org.gbif.pipelines.validator.ws;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
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
  ValidatorResponse validateArchive(@RequestParam("callback") String callbackUrl, byte[] file);

  @Data
  @JsonIgnoreProperties(ignoreUnknown = true)
  class ValidatorResponse {
    private int key;
  }

  @GetMapping(path = "dataset/{key}/import")
  List<ImportResponse> checkImport(@PathVariable("key") int key);

  @Data
  @JsonIgnoreProperties(ignoreUnknown = true)
  class ImportResponse {
    private int datasetKey;
    private String status;
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
