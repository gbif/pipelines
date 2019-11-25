package org.gbif.pipelines.parsers.ws.client.metadata.contentful;

import org.gbif.pipelines.parsers.ws.client.metadata.response.Program;
import org.gbif.pipelines.parsers.ws.client.metadata.response.Project;

import java.util.Objects;

import com.contentful.java.cda.CDAArray;
import com.contentful.java.cda.CDAClient;
import com.contentful.java.cda.CDAEntry;
import com.contentful.java.cda.QueryOperation;

/**
 * Client service to Contentful CMS service.
 */
public class ContentfulService {

  private CDAClient cdaClient;
  private static final String DEFAULT_LOCALE = "en-GB";

  /**
   *
   * @param authToken contentful auth token
   * @param spaceId contentful space id
   */
  public ContentfulService(String authToken, String spaceId) {
    cdaClient = CDAClient
                  .builder()
                  .setToken(authToken) // required
                  .setSpace(spaceId) // required
                  .build();
  }

  /**
   * Gets a project by its projectId field in Contentful.
   * @param projectId to be queried
   * @return a project linked to the identifier, null otherwise
   */
  public Project getProject(String projectId) {
    CDAArray projects = cdaClient
                    .fetch(CDAEntry.class)
                    .include(2) //Go two levels deep in nested entities
                    .withContentType("Project")
                    // `fields.brand` is a reference to a specific field called `brand` in you `content type`.
                    .where("fields.projectId", QueryOperation.IsEqualTo, projectId)
                    .limit(1) //Just want the first match
                    .all();
    if (projects.total() > 0) {
      CDAEntry projectResource = (CDAEntry)projects.items().get(0);
      return new Project(projectResource.getField(DEFAULT_LOCALE, "title"),
                         projectResource.getField("projectId"),
                         getProgramme(projectResource));
    }
    return null;
  }

  /** Converts a project entry/resource into a Programme object.
   * Returns null if the project doesn't have an associated program.*/
  private Program getProgramme(CDAEntry projectEntry) {
    Object programmeO = projectEntry.getField("programme");
    if (Objects.nonNull(programmeO)) {
      CDAEntry programmeEntry = (CDAEntry)programmeO;
      return new Program(programmeEntry.getAttribute("id"), //id system attribute
                         programmeEntry.getField(DEFAULT_LOCALE, "title"),
                         programmeEntry.getField("acronym"));
    }
    return null;
  }



  public static void main(String[] args) {
    // Create the Contentful client.
    ContentfulService service = new ContentfulService("942d3f5d46493fc2d86519b19de88f233615bf4d8cb3439faa6cdebbb8ce4aa3","qz96qz790wh9");
    Project project = service.getProject("BID-AF2015-0065-NAC");
    System.out.println(project);
  }
 }
