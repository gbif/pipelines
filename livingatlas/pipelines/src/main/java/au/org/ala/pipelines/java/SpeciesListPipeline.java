package au.org.ala.pipelines.java;

import static java.util.stream.Collectors.groupingBy;
import static org.gbif.pipelines.common.PipelinesVariables.Pipeline.AVRO_EXTENSION;

import au.org.ala.pipelines.beam.IndexRecordPipeline;
import au.org.ala.pipelines.options.SpeciesLevelPipelineOptions;
import au.org.ala.pipelines.transforms.ALATaxonomyTransform;
import au.org.ala.pipelines.util.SpeciesListUtils;
import au.org.ala.pipelines.util.VersionInfo;
import au.org.ala.specieslists.SpeciesListDownloader;
import au.org.ala.utils.CombinedYamlConfiguration;
import java.io.OutputStream;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.function.UnaryOperator;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.io.DatumWriter;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.gbif.pipelines.common.PipelinesException;
import org.gbif.pipelines.common.beam.options.PipelinesOptionsFactory;
import org.gbif.pipelines.common.beam.utils.PathBuilder;
import org.gbif.pipelines.core.io.AvroReader;
import org.gbif.pipelines.core.utils.FsUtils;
import org.gbif.pipelines.io.avro.ALATaxonRecord;
import org.gbif.pipelines.io.avro.SpeciesListRecord;
import org.gbif.pipelines.io.avro.TaxonProfile;

/**
 * Java based species list pipeline which will download the species list information and create a
 * TaxonProfile extension for a dataset, which contains:
 *
 * <ul>
 *   <li>Links to species lists for records
 *   <li>stateProvince and country associated conservation status for the record
 *   <li>stateProvince and country associated invasive status for the record
 * </ul>
 *
 * This pipeline is left for debug purposes only. Species lists are joined to the records in the
 * {@link IndexRecordPipeline} so there is no need to run this pipeline separately.
 *
 * @see TaxonProfile
 * @see SpeciesListDownloader
 */
@Slf4j
public class SpeciesListPipeline {

  public static void main(String[] args) throws Exception {
    VersionInfo.print();
    String[] combinedArgs = new CombinedYamlConfiguration(args).toArgs("general", "speciesLists");
    SpeciesLevelPipelineOptions options =
        PipelinesOptionsFactory.create(SpeciesLevelPipelineOptions.class, combinedArgs);
    PipelinesOptionsFactory.registerHdfs(options);
    run(options);
  }

  public static void run(SpeciesLevelPipelineOptions options) throws Exception {

    log.info("Creating a pipeline from options");
    Map<String, TaxonProfile> taxonProfilesCollection = generateTaxonProfileCollection(options);

    // construct output path
    String avroPath =
        String.join(
            "/",
            options.getInputPath(),
            options.getDatasetId(),
            options.getAttempt().toString(),
            "taxon_profiles",
            "taxon-profile-record");

    // get filesystem
    FileSystem fs =
        FsUtils.getFileSystem(
            options.getHdfsSiteConfig(), options.getCoreSiteConfig(), options.getInputPath());

    DatumWriter<TaxonProfile> datumWriter = new GenericDatumWriter<>(TaxonProfile.getClassSchema());
    try (OutputStream output = fs.create(new Path(avroPath));
        DataFileWriter<TaxonProfile> dataFileWriter = new DataFileWriter<>(datumWriter)) {
      dataFileWriter.create(TaxonProfile.getClassSchema(), output);

      Collection<TaxonProfile> profiles = taxonProfilesCollection.values();
      for (TaxonProfile profile : profiles) {
        dataFileWriter.append(profile);
      }
    }
    log.info("Completed species list pipeline for dataset {}", options.getDatasetId());
  }

  /** Generate a PCollection of taxon profiles. */
  public static Map<String, TaxonProfile> generateTaxonProfileCollection(
      SpeciesLevelPipelineOptions options) {

    try {
      log.info("Download species lists");
      SpeciesListDownloader.run(options);

      log.info("Running species list pipeline for dataset {}", options.getDatasetId());
      UnaryOperator<String> pathFn =
          t -> PathBuilder.buildPathInterpretUsingTargetPath(options, t, "*" + AVRO_EXTENSION);

      List<SpeciesListRecord> speciesListRecords =
          AvroReader.readObjects(
              options.getHdfsSiteConfig(),
              options.getCoreSiteConfig(),
              SpeciesListRecord.class,
              options.getSpeciesAggregatesPath() + options.getSpeciesListCachePath());

      // transform to taxonID -> List<SpeciesListRecord>
      Map<String, List<SpeciesListRecord>> speciesListMap =
          speciesListRecords.stream().collect(groupingBy(SpeciesListRecord::getTaxonID));

      List<ALATaxonRecord> alaTaxonRecords =
          AvroReader.readObjects(
              options.getHdfsSiteConfig(),
              options.getCoreSiteConfig(),
              ALATaxonRecord.class,
              pathFn.apply(ALATaxonomyTransform.builder().create().getBaseName()));

      // join by taxonID
      List<TaxonProfile> profiles =
          alaTaxonRecords.stream()
              .map(
                  alaTaxonRecord ->
                      convertToTaxonProfile(
                          alaTaxonRecord,
                          speciesListMap,
                          options.getIncludeConservationStatus(),
                          options.getIncludeInvasiveStatus()))
              .collect(Collectors.toList());

      return profiles.stream()
          .filter(taxonProfile -> taxonProfile != null && taxonProfile.getId() != null)
          .collect(Collectors.toMap(TaxonProfile::getId, taxon -> taxon));
    } catch (Exception e) {
      throw new PipelinesException(e);
    }
  }

  static TaxonProfile convertToTaxonProfile(
      ALATaxonRecord alaTaxonRecord,
      Map<String, List<SpeciesListRecord>> speciesListMap,
      boolean includeConservationStatus,
      boolean includeInvasiveStatus) {

    Iterable<SpeciesListRecord> speciesLists =
        speciesListMap.get(alaTaxonRecord.getTaxonConceptID());

    if (speciesLists != null) {
      TaxonProfile.Builder builder =
          SpeciesListUtils.createTaxonProfileBuilder(
              speciesLists, includeConservationStatus, includeInvasiveStatus);
      builder.setId(alaTaxonRecord.getId());
      return builder.build();
    } else {
      return null;
    }
  }
}
