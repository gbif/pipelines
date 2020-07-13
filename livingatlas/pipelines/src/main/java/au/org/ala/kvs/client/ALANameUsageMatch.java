package au.org.ala.kvs.client;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonPOJOBuilder;
import lombok.Builder;
import lombok.Value;

import java.util.List;

@JsonDeserialize(builder = ALANameUsageMatch.ALANameUsageMatchBuilder.class)
@Value
@Builder
public class ALANameUsageMatch {

  private boolean success;
  private String scientificName;
  private String scientificNameAuthorship;
  private String taxonConceptID;
  private String rank;
  private Integer rankID;
  private Integer lft;
  private Integer rgt;
  private String matchType;
  private String kingdom;
  private String kingdomID;
  private String phylum;
  private String phylumID;
  private String classs;
  private String classID;
  private String order;
  private String orderID;
  private String family;
  private String familyID;
  private String genus;
  private String genusID;
  private String species;
  private String speciesID;
  private String vernacularName;
  private List<String> speciesGroup;
  private List<String> speciesSubgroup;

  @JsonPOJOBuilder(withPrefix = "")
  public static class ALANameUsageMatchBuilder {

  }

  public static final ALANameUsageMatch FAIL = ALANameUsageMatch.builder().success(false).build();
}
