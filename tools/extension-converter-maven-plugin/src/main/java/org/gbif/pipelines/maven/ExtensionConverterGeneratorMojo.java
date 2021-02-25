package org.gbif.pipelines.maven;

import static java.nio.charset.StandardCharsets.UTF_8;

import freemarker.template.Configuration;
import freemarker.template.TemplateExceptionHandler;
import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.maven.plugin.AbstractMojo;
import org.apache.maven.plugin.MojoExecutionException;
import org.apache.maven.plugins.annotations.LifecyclePhase;
import org.apache.maven.plugins.annotations.Mojo;
import org.apache.maven.plugins.annotations.Parameter;
import org.gbif.dwc.digester.ThesaurusHandlingRule;
import org.gbif.dwc.extensions.Extension;
import org.gbif.dwc.extensions.ExtensionFactory;
import org.gbif.dwc.extensions.VocabulariesManager;
import org.gbif.dwc.extensions.Vocabulary;
import org.gbif.dwc.xml.SAXUtils;

@Mojo(name = "extensiongeneration", defaultPhase = LifecyclePhase.GENERATE_SOURCES)
public class ExtensionConverterGeneratorMojo extends AbstractMojo {

  @Parameter(property = "extensiongeneration.extensions")
  private List<String> extensions;

  @Parameter(property = "extensiongeneration.pathToWrite")
  private String pathToWrite;

  @Parameter(property = "extensiongeneration.namespace")
  private String namespace;

  @Override
  public void execute() throws MojoExecutionException {

    try {
      e();
      //      Files.createDirectories(Paths.get(pathToWrite));
      //
      //      for (String extension : extensions) {
      //
      //        String[] ext = extension.split(",");
      //        URL url = new URL(ext[1]);
      //        String name = ext[0];
      //        Path path = Paths.get(pathToWrite, normalizeFileName(name));
      //        if (!Files.exists(path)) {
      //          convertAndWrite(name, url, path);
      //        }
      //      }
    } catch (Exception ex) {
      throw new MojoExecutionException(ex.getMessage());
    }
  }

  private void e() throws IOException {
    // Create your Configuration instance, and specify if up to what FreeMarker
    // version (here 2.3.29) do you want to apply the fixes that are not 100%
    // backward-compatible. See the Configuration JavaDoc for details.
    Configuration cfg = new Configuration(Configuration.VERSION_2_3_31);

    // Specify the source where the template files come from. Here I set a
    // plain directory for it, but non-file-system sources are possible too:
    String url = getClass().getResource("/").getFile();
    cfg.setDirectoryForTemplateLoading(new File(url));

    // From here we will set the settings recommended for new projects. These
    // aren't the defaults for backward compatibilty.

    // Set the preferred charset template files are stored in. UTF-8 is
    // a good choice in most applications:
    cfg.setDefaultEncoding(UTF_8.toString());

    // Sets how errors will appear.
    // During web page *development* TemplateExceptionHandler.HTML_DEBUG_HANDLER is better.
    cfg.setTemplateExceptionHandler(TemplateExceptionHandler.RETHROW_HANDLER);

    // Don't log exceptions inside FreeMarker that it will thrown at you anyway:
    cfg.setLogTemplateExceptions(false);

    // Wrap unchecked exceptions thrown during template processing into TemplateException-s:
    cfg.setWrapUncheckedExceptions(true);

    // Do not fall back to higher scopes when reading a null loop variable:
    cfg.setFallbackOnNullLoopVariable(false);
  }

  public void setExtensions(List<String> extensions) {
    this.extensions = extensions;
  }

  public void setPathToWrite(String pathToWrite) {
    this.pathToWrite = pathToWrite;
  }

  public void setNamespace(String namespace) {
    this.namespace = namespace;
  }

  private void convertAndWrite(String name, URL url, Path path) throws Exception {
    // Read extension
    ThesaurusHandlingRule thr = new ThesaurusHandlingRule(new EmptyVocabulariesManager());
    ExtensionFactory factory = new ExtensionFactory(thr, SAXUtils.getNsAwareSaxParserFactory());
    Extension ext = factory.build(url.openStream(), url, false);
  }

  private String nomalizeFieldName(String name) {
    return name.toLowerCase().trim().replace('-', '_');
  }

  private String normalizeFileName(String name) {
    String result =
        Arrays.stream(name.split("(?=[A-Z])"))
            .map(String::toLowerCase)
            .collect(Collectors.joining("-"));
    return result + ".avsc";
  }

  private static class EmptyVocabulariesManager implements VocabulariesManager {

    @Override
    public Vocabulary get(String uri) {
      return null;
    }

    @Override
    public Vocabulary get(URL url) {
      return null;
    }

    @Override
    public Map<String, String> getI18nVocab(String uri, String lang) {
      return null;
    }

    @Override
    public List<Vocabulary> list() {
      return null;
    }
  }
}
