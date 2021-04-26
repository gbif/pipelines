package au.org.ala.pipelines.util;

import com.google.common.base.Strings;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import org.gbif.pipelines.io.avro.*;

/** Utility code for integration with species lists. */
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class SpeciesListUtils {

  /**
   * Creates a reusable template (Builder) for a TaxonProfile based on the supplied species lists.
   */
  public static TaxonProfile.Builder createTaxonProfileBuilder(
      Iterable<SpeciesListRecord> speciesLists,
      boolean includeConservationStatus,
      boolean includeInvasiveStatus) {

    Iterator<SpeciesListRecord> iter = speciesLists.iterator();

    List<String> speciesListIDs = new ArrayList<>();
    List<ConservationStatus> conservationStatusList = new ArrayList<>();
    List<InvasiveStatus> invasiveStatusList = new ArrayList<>();

    while (iter.hasNext()) {

      SpeciesListRecord speciesListRecord = iter.next();
      speciesListIDs.add(speciesListRecord.getSpeciesListID());

      if (includeConservationStatus
          && speciesListRecord.getIsThreatened()
          && (!Strings.isNullOrEmpty(speciesListRecord.getSourceStatus())
              || !Strings.isNullOrEmpty(speciesListRecord.getStatus()))) {
        conservationStatusList.add(
            ConservationStatus.newBuilder()
                .setSpeciesListID(speciesListRecord.getSpeciesListID())
                .setRegion(speciesListRecord.getRegion())
                .setSourceStatus(speciesListRecord.getSourceStatus())
                .setStatus(speciesListRecord.getStatus())
                .build());
      } else if (includeInvasiveStatus && speciesListRecord.getIsInvasive()) {
        invasiveStatusList.add(
            InvasiveStatus.newBuilder()
                .setSpeciesListID(speciesListRecord.getSpeciesListID())
                .setRegion(speciesListRecord.getRegion())
                .build());
      }
    }

    // output a link to each occurrence record we've matched by taxonID
    TaxonProfile.Builder builder = TaxonProfile.newBuilder();
    builder.setSpeciesListID(speciesListIDs);
    builder.setConservationStatuses(conservationStatusList);
    builder.setInvasiveStatuses(invasiveStatusList);
    return builder;
  }
}
