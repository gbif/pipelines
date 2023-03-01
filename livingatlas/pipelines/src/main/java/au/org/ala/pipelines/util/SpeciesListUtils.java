package au.org.ala.pipelines.util;

import com.google.common.base.Strings;
import java.util.*;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import org.gbif.pipelines.io.avro.*;

/** Utility code for integration with species lists. */
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class SpeciesListUtils {

  private static String LIST_COMMON_TRAIT = "COMMON_TRAIT";

  /**
   * Creates a reusable template (Builder) for a TaxonProfile based on the supplied species lists.
   */
  public static TaxonProfile.Builder createTaxonProfileBuilder(
      Iterable<SpeciesListRecord> speciesLists,
      boolean includeConservationStatus,
      boolean includeInvasiveStatus,
      boolean includePresentInCountry,
      boolean includeTraits) {

    Iterator<SpeciesListRecord> iter = speciesLists.iterator();

    List<String> speciesListIDs = new ArrayList<>();
    List<ConservationStatus> conservationStatusList = new ArrayList<>();
    List<InvasiveStatus> invasiveStatusList = new ArrayList<>();
    String presentInCountryValue = null;
    Map<String, String> traitsMap = new HashMap<>();

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
      } else if (includePresentInCountry && speciesListRecord.getPresentInCountry() != null) {
        presentInCountryValue = speciesListRecord.getPresentInCountry();
      } else if (includeTraits
          && speciesListRecord.getListType().equals(LIST_COMMON_TRAIT)
          && speciesListRecord.getTraitName() != null) {
        traitsMap.put(speciesListRecord.getTraitName(), speciesListRecord.getTraitValue());
      }
    }

    // output a link to each occurrence record we've matched by taxonID
    TaxonProfile.Builder builder = TaxonProfile.newBuilder();
    builder.setSpeciesListID(speciesListIDs);
    builder.setConservationStatuses(conservationStatusList);
    builder.setInvasiveStatuses(invasiveStatusList);
    builder.setPresentInCountry(presentInCountryValue);
    builder.setTraits(traitsMap);
    return builder;
  }
}
