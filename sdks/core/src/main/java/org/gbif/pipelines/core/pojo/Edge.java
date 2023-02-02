package org.gbif.pipelines.core.pojo;

import java.io.Serializable;
import lombok.AllArgsConstructor;
import lombok.Data;

/**
 * Graph edge to simplify the traversal of parent -> child relations.
 *
 * @param <E> content of the relation between fromId to toId
 */
@Data
@AllArgsConstructor(staticName = "of")
public class Edge<E> implements Serializable {

  private String fromId;
  private String toId;
  private E record;

  public static <E> Edge<E> of(String fromId, String toId) {
    return of(fromId, toId, null);
  }
}
