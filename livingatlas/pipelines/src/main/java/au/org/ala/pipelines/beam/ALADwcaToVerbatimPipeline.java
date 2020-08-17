package au.org.ala.pipelines.beam;

import au.org.ala.utils.CombinedYamlConfiguration;
import java.io.FileNotFoundException;
import lombok.extern.slf4j.Slf4j;
import org.gbif.api.model.pipelines.StepType;
import org.gbif.pipelines.ingest.options.InterpretationPipelineOptions;
import org.gbif.pipelines.ingest.options.PipelinesOptionsFactory;
import org.gbif.pipelines.ingest.pipelines.DwcaToVerbatimPipeline;
import org.slf4j.MDC;

/** Thin wrapper around DwcaToVerbatimPipeline to allow for Yaml config setup. */
@Slf4j
public class ALADwcaToVerbatimPipeline {

  public static void main(String[] args) throws FileNotFoundException {
    String[] combinedArgs = new CombinedYamlConfiguration(args).toArgs("general", "dwca-avro");
    InterpretationPipelineOptions options =
        PipelinesOptionsFactory.createInterpretation(combinedArgs);
    MDC.put("datasetId", options.getDatasetId());
    MDC.put("attempt", options.getAttempt().toString());
    MDC.put("step", StepType.DWCA_TO_VERBATIM.name());
    DwcaToVerbatimPipeline.run(options);
  }
}
