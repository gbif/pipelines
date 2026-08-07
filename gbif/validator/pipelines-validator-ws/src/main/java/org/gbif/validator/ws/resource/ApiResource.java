package org.gbif.validator.ws.resource;

import io.swagger.v3.oas.annotations.Hidden;
import jakarta.servlet.http.HttpServletResponse;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class ApiResource {

  @Hidden
  @GetMapping("/")
  public void index(HttpServletResponse response) throws Exception {
    response.sendRedirect("/swagger-ui/index.html");
  }
}
