package au.org.ala.pipelines.java;

import static java.util.stream.Collectors.groupingBy;
import static org.gbif.pipelines.common.PipelinesVariables.Pipeline.AVRO_EXTENSION;

import au.org.ala.pipelines.options.SpeciesLevelPipelineOptions;
import au.org.ala.pipelines.transforms.ALATaxonomyTransform;
import au.org.ala.pipelines.util.VersionInfo;
import au.org.ala.specieslists.SpeciesListDownloader;
import au.org.ala.utils.CombinedYamlConfiguration;
import com.google.common.base.Strings;
import java.io.OutputStream;
import java.util.*;
import java.util.function.UnaryOperator;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.file.CodecFactory;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.io.DatumWriter;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.gbif.pipelines.common.beam.options.PipelinesOptionsFactory;
import org.gbif.pipelines.common.beam.utils.PathBuilder;
import org.gbif.pipelines.core.io.AvroReader;
import org.gbif.pipelines.core.utils.FsUtils;
import org.gbif.pipelines.io.avro.*;

@Slf4j
public class SpeciesListPipeline {

  private static final CodecFactory BASE_CODEC = CodecFactory.snappyCodec();

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
            "taxonprofiles",
            "taxon-profile-record");

    // get filesystem
    FileSystem fs =
        FsUtils.getFileSystem(
            options.getHdfsSiteConfig(), options.getCoreSiteConfig(), options.getInputPath());

    OutputStream output = fs.create(new Path(avroPath));
    DatumWriter<TaxonProfile> datumWriter = new GenericDatumWriter<>(TaxonProfile.getClassSchema());
    DataFileWriter dataFileWriter = new DataFileWriter<TaxonProfile>(datumWriter);
    dataFileWriter.create(TaxonProfile.getClassSchema(), output);

    Collection<TaxonProfile> profiles = taxonProfilesCollection.values();
    for (TaxonProfile profile : profiles) {
      dataFileWriter.append(profile);
    }

    dataFileWriter.close();
    log.info("Completed species list pipeline for dataset {}", options.getDatasetId());
  }

  /**
   * Generate a PCollection of taxon profiles.
   *
   * @param options
   * @return
   */
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
              options.getSpeciesAggregatesPath() + "/species-lists/species-lists.avro");

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
              .map(alaTaxonRecord -> convertToTaxonProfile(alaTaxonRecord, speciesListMap))
              .collect(Collectors.toList());

      return profiles.stream()
          .filter(taxonProfile -> taxonProfile != null && taxonProfile.getId() != null)
          .collect(Collectors.toMap(TaxonProfile::getId, taxon -> taxon));
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }


  static TaxonProfile convertToTaxonProfile(
      ALATaxonRecord alaTaxonRecord, Map<String, List<SpeciesListRecord>> speciesListMap) {

    Iterable<SpeciesListRecord> speciesLists =
        speciesListMap.get(alaTaxonRecord.getTaxonConceptID());

    if (speciesLists != null) {
      Iterator<SpeciesListRecord> iter = speciesLists.iterator();

      List<String> speciesListIDs = new ArrayList<String>();
      List<ConservationStatus> conservationStatusList = new ArrayList<ConservationStatus>();
      List<InvasiveStatus> invasiveStatusList = new ArrayList<InvasiveStatus>();

      while (iter.hasNext()) {

        SpeciesListRecord speciesListRecord = iter.next();
        speciesListIDs.add(speciesListRecord.getSpeciesListID());

        if (speciesListRecord.getIsThreatened()
            && (!Strings.isNullOrEmpty(speciesListRecord.getSourceStatus())
                || !Strings.isNullOrEmpty(speciesListRecord.getStatus()))) {
          conservationStatusList.add(
              ConservationStatus.newBuilder()
                  .setSpeciesListID(speciesListRecord.getSpeciesListID())
                  .setRegion(speciesListRecord.getRegion())
                  .setSourceStatus(speciesListRecord.getSourceStatus())
                  .setStatus(speciesListRecord.getStatus())
                  .build());
        } else if (speciesListRecord.getIsInvasive()) {
          invasiveStatusList.add(
              InvasiveStatus.newBuilder()
                  .setSpeciesListID(speciesListRecord.getSpeciesListID())
                  .setRegion(speciesListRecord.getRegion())
                  .build());
        }
      }

      // output a link to each occurrence record we've matched by taxonID
      TaxonProfile.Builder builder = TaxonProfile.newBuilder();
      builder.setId(alaTaxonRecord.getId());
      builder.setSpeciesListID(speciesListIDs);
      builder.setConservationStatuses(conservationStatusList);
      builder.setInvasiveStatuses(invasiveStatusList);
      return builder.build();
    } else {
      return null;
    }
  }
}
