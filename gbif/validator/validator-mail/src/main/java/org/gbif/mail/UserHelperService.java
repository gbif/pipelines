package org.gbif.mail;

import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.Optional;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.gbif.api.model.common.AbstractGbifUser;
import org.gbif.api.model.common.GbifUser;
import org.gbif.api.service.common.IdentityAccessService;
import org.springframework.stereotype.Service;

/** Helper service to encapsulate operations related to get users' data. */
@Service
@RequiredArgsConstructor
@Slf4j
public class UserHelperService {

  // supported locales
  private static final List<String> SUPPORTED_LOCALES = Arrays.asList("en", "ru", "es");

  private final IdentityAccessService identityAccessService;

  /** User lookup by username. */
  public Optional<GbifUser> getUser(String username) {
    Optional<GbifUser> user = Optional.ofNullable(identityAccessService.get(username));
    if (!user.isPresent()) {
      log.warn("User with name [{}] was not found!", username);
    }
    return user;
  }

  /** Gets the user local, if not found returns the default English Locale. */
  public Locale getUserLocaleOrDefault(GbifUser user) {
    log.debug("Get creator's locale. Creator: {}", user);
    Locale locale =
        Optional.ofNullable(user)
            .map(AbstractGbifUser::getLocale)
            .map(UserHelperService::findLocaleTag)
            .map(Locale::forLanguageTag)
            .orElse(Locale.ENGLISH);

    log.debug("Creator's locale is [{}]", locale);
    return locale;
  }

  /** Tries to find the Locale tag. */
  private static String findLocaleTag(Locale locale) {
    log.debug("Trying to find a suitable locale tag for locale [{}]", locale);
    String localeTag =
        Locale.lookupTag(Locale.LanguageRange.parse(locale.toLanguageTag()), SUPPORTED_LOCALES);
    log.debug("Use locale tag [{}]", localeTag);
    return localeTag;
  }
}
