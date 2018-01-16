package org.gbif.dwca.generator.util;

import org.gbif.dwca.Category;
import org.gbif.dwca.DwCATerm;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

/**
 * This is a HTML Parsing tool which helps to parse the DwCA Spec and generate a common data model
 * for any DwCA schema builders. Note: The tool may break in future if the format or the URL
 * http://tdwg.github.io/dwc/terms/index.htm is changed
 *
 * @author clf358
 */
@SuppressWarnings({"WhileLoopReplaceableByForEach", "SpellCheckingInspection"})
public class DwCAHTMLParsingTool {

  private static final Logger LOG = LogManager.getLogger(DwCAHTMLParsingTool.class);

  private final String TERM_INDEX_PAGE = "http://tdwg.github.io/dwc/terms/index.htm";
  private final String TBODY_TAG = "tbody";
  private final String TR_TAG = "tr";
  private final String TD_TAG = "td";
  private final String TBODY_COMMENT = "<!-- Begin Terms Table -->";
  private final String A_TAG = "a";
  private final String NAME_ATTR = "name";
  private final int INFO_PER_TERM = 6;
  private final String RECORD_LEVEL_TERM = "all";

  private DwCADataModel dataModel = null;

  /**
   * parse the provided Spec from @see <a href=
   * "http://tdwg.github.io/dwc/terms/index.htm">http://tdwg.github.io/dwc/terms/index.htm</a>.
   * Generates a data model for helping schema builders.
   */
  public DwCADataModel parseAndGenerateModel() throws IOException {
    dataModel = new DwCADataModel();
    Document doc = Jsoup.connect(TERM_INDEX_PAGE).get();
    Elements elem = doc.getElementsByTag(TBODY_TAG);
    Iterator<Element> iter = elem.iterator();

    while (iter.hasNext()) {
      Element e = iter.next();
      if (e.html().startsWith(TBODY_COMMENT)) {
        Elements infos = e.getElementsByTag(TR_TAG);
        if (infos.size() % INFO_PER_TERM == 0) {
          int i = 0;

          while (i < infos.size()) {

            String termName = infos.get(i).getElementsByTag(A_TAG).attr(NAME_ATTR);
            i++;

            String identifier = infos.get(i).getElementsByTag(TD_TAG).get(1).text();
            i++;

            String classOfTerm = infos.get(i).getElementsByTag(TD_TAG).get(1).text();
            i++;

            String definition = infos.get(i).getElementsByTag(TD_TAG).get(1).text();
            i++;

            String comment = infos.get(i).getElementsByTag(TD_TAG).get(1).text();
            i++;

            String details = infos.get(i).getElementsByTag(TD_TAG).get(1).text();
            i++;

            DwCATerm t = new DwCATerm(termName, classOfTerm, identifier, definition, comment, details);

            if (t.isCategory()) {
              dataModel.getIdentifierCategoryMap().put(t.getIdentifier(), t);
              dataModel.getCategories().add(t);
              dataModel.getCategoryIdentifierAndFieldsMap().put(t.getIdentifier(), new ArrayList<>());
              if (LOG.isDebugEnabled()) LOG.debug("Adding category :" + t);
            } else {
              if (LOG.isDebugEnabled()) {
                LOG.debug("Adding field :" + t + " to the category" + dataModel.getIdentifierCategoryMap()
                  .get(t.getClassOfTerm()));
              }
              if (dataModel.getCategoryIdentifierAndFieldsMap().containsKey(t.getClassOfTerm())) {
                dataModel.getCategoryIdentifierAndFieldsMap().get(t.getClassOfTerm()).add(t);
              } else {
                List<DwCATerm> terms = new ArrayList<>();
                terms.add(t);
                dataModel.getCategoryIdentifierAndFieldsMap().put(t.getClassOfTerm(), terms);
              }
            }

            dataModel.getListOfAllTerms().add(t);

          }
        }

        List<DwCATerm> allTerms = dataModel.getCategoryIdentifierAndFieldsMap().get(RECORD_LEVEL_TERM);
        dataModel.getCategoryIdentifierAndFieldsMap().remove(RECORD_LEVEL_TERM);
        Iterator<Entry<String, List<DwCATerm>>> iterator =
          dataModel.getCategoryIdentifierAndFieldsMap().entrySet().iterator();
        while (iterator.hasNext()) {
          Entry<String, List<DwCATerm>> entry = iterator.next();
          dataModel.getCategoryIdentifierAndFieldsMap().get(entry.getKey()).addAll(allTerms);
        }
        createExtendedCoreCategory();
      }

    }
    return dataModel;
  }

  /**
   * adds the new custom category to the created model for generating extended occurence record.
   */
  private void createExtendedCoreCategory() {
    DwCATerm extendedOccurence = new DwCATerm(Category.ExtendedOccurence.name(),
                                              "",
                                              Category.ExtendedOccurence.getCategoryIdentifier(),
                                              "Extended occurence with fields from other terms of reference",
                                              "Custom Category",
                                              "Custom category for parsing core of DwCAcore archive");
    dataModel.getCategories().add(extendedOccurence);

    dataModel.getIdentifierCategoryMap().put(extendedOccurence.getIdentifier(), extendedOccurence);
    dataModel.getCategoryIdentifierAndFieldsMap().put(extendedOccurence.getIdentifier(), new ArrayList<>());

    Iterator<Entry<String, List<DwCATerm>>> entries =
      dataModel.getCategoryIdentifierAndFieldsMap().entrySet().iterator();
    Set<DwCATerm> extendedTerms = new HashSet<>();
    while (entries.hasNext()) {
      Entry<String, List<DwCATerm>> entry = entries.next();
      if (!entry.getKey().contains("MeasurementOrFact") || !entry.getKey().contains("ResourceRelationship")) {
        extendedTerms.addAll(entry.getValue());
      }
    }
    dataModel.getCategoryIdentifierAndFieldsMap().get(extendedOccurence.getIdentifier()).addAll(extendedTerms);

  }

}
