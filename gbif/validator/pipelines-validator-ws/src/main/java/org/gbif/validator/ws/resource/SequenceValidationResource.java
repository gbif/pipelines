package org.gbif.validator.ws.resource;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.media.Content;
import io.swagger.v3.oas.annotations.media.Schema;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.responses.ApiResponses;
import io.swagger.v3.oas.annotations.tags.Tag;
import org.gbif.dna.core.SequenceProcessor;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@Tag(name = "Sequence Validation", description = "Sequence validation operations")
@RestController
@RequestMapping(value = "validation/sequence", produces = MediaType.APPLICATION_JSON_VALUE)
public class SequenceValidationResource {

  private final SequenceProcessor sequenceProcessor = new SequenceProcessor();

  @Operation(
      summary = "Validate sequence",
      description = "Validate a single DNA sequence and return processing results.")
  @ApiResponses({
    @ApiResponse(
        responseCode = "200",
        description = "Sequence processed",
        content = @Content(schema = @Schema(implementation = SequenceProcessor.Result.class))),
    @ApiResponse(responseCode = "400", description = "Invalid sequence")
  })
  @GetMapping
  public SequenceProcessor.Result validateSequence(
      @Parameter(description = "Sequence string") @RequestParam("sequence") String sequence) {
    Preconditions.checkArgument(!Strings.isNullOrEmpty(sequence));

    return sequenceProcessor.processOneSequence(sequence);
  }
}
