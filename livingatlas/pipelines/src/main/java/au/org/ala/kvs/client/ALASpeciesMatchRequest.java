package au.org.ala.kvs.client;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonPOJOBuilder;
import lombok.Builder;
import lombok.ToString;
import lombok.Value;
import org.gbif.kvs.species.SpeciesMatchRequest;


/**
 * This is a copy of org.gbif.kvs.species.SpeciesMatchRequest with taxonConceptID, taxonID added <p>
 * TODO remove, consolidate with GBIF API
 */
@JsonDeserialize(builder = ALASpeciesMatchRequest.ALASpeciesMatchRequestBuilder.class)
@Value
@Builder
@ToString
public class ALASpeciesMatchRequest {

  private final String kingdom;
  private final String phylum;
  private final String clazz;
  private final String order;
  private final String family;
  private final String genus;
  private final String specificEpithet;
  private final String infraspecificEpithet;
  private final String rank;
  private final String verbatimTaxonRank;
  private final String scientificName;
  private final String genericName;
  private final String scientificNameAuthorship;
  private final String taxonConceptID;
  private final String taxonID;

  @JsonPOJOBuilder(withPrefix = "")
  public static class ALASpeciesMatchRequestBuilder {

  }

}
