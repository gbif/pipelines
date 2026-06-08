package org.gbif.validator.ws.resource;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import org.gbif.dna.core.SequenceProcessor;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping(value = "validation/sequence", produces = MediaType.APPLICATION_JSON_VALUE)
public class SequenceValidationResource {

  private final SequenceProcessor sequenceProcessor = new SequenceProcessor();

  @GetMapping
  public SequenceProcessor.Result validateSequence(@RequestParam("sequence") String sequence) {
    Preconditions.checkArgument(!Strings.isNullOrEmpty(sequence));

    return sequenceProcessor.processOneSequence(sequence);
  }
}
