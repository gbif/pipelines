package org.gbif.pipelines.core.ws.metadata.response;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.io.Serial;
import java.io.Serializable;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

/** A phylogenetic tree file published with a dataset, as listed by the registry. */
@Getter
@Setter
@Builder
@NoArgsConstructor
@AllArgsConstructor
@JsonIgnoreProperties(ignoreUnknown = true)
public class PhylogenyFile implements Serializable {

  @Serial private static final long serialVersionUID = 4520671538102733514L;

  private String fileName;
  private Long size;

  /** NEWICK or NEXUS */
  private String format;

  /** Parsing error reported by the registry, if any */
  private String error;

  private boolean treesTruncated;

  @JsonProperty("isValid")
  private boolean valid;
}
