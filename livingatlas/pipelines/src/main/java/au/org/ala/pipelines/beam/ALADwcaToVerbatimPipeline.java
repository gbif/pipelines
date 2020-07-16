package au.org.ala.pipelines.beam;

import au.org.ala.utils.CombinedYamlConfiguration;
import java.io.FileNotFoundException;
import lombok.extern.slf4j.Slf4j;
import org.gbif.pipelines.ingest.options.InterpretationPipelineOptions;
import org.gbif.pipelines.ingest.options.PipelinesOptionsFactory;
import org.gbif.pipelines.ingest.pipelines.DwcaToVerbatimPipeline;

/** Thin wrapper around DwcaToVerbatimPipeline to allow for Yaml config setup. */
@Slf4j
public class ALADwcaToVerbatimPipeline {

  public static void main(String[] args) throws FileNotFoundException {
    String[] combinedArgs = new CombinedYamlConfiguration(args).toArgs("general", "dwca-avro");
    InterpretationPipelineOptions options =
        PipelinesOptionsFactory.createInterpretation(combinedArgs);
    DwcaToVerbatimPipeline.run(options);
  }
}
