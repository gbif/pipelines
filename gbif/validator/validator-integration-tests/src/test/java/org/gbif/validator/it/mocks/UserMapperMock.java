package org.gbif.validator.it.mocks;

import jakarta.annotation.Nullable;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.gbif.api.model.common.GbifUser;
import org.gbif.api.model.common.paging.Pageable;
import org.gbif.api.vocabulary.Country;
import org.gbif.api.vocabulary.UserRole;
import org.gbif.registry.persistence.mapper.UserMapper;

public class UserMapperMock implements UserMapper {

  private final Map<String, GbifUser> users = new HashMap<>();

  private final AtomicInteger keyGen = new AtomicInteger();

  @Override
  public void create(GbifUser gbifUser) {
    gbifUser.setKey(keyGen.incrementAndGet());
    users.put(gbifUser.getUserName(), gbifUser);
  }

  @Override
  public GbifUser get(String userName) {
    return users.get(userName);
  }

  @Override
  public GbifUser getByKey(int key) {
    return users.values().stream().filter(user -> key == user.getKey()).findFirst().orElse(null);
  }

  @Override
  public GbifUser getByEmail(String email) {
    return users.values().stream()
        .filter(user -> email.equals(user.getEmail()))
        .findFirst()
        .orElse(null);
  }

  @Override
  public GbifUser getBySystemSetting(String key, String value) {
    return users.values().stream()
        .filter(user -> value.equals(user.getSystemSettings().get(key)))
        .findFirst()
        .orElse(null);
  }

  @Override
  public void updateLastLogin(int key) {
    Optional.ofNullable(getByKey(key)).ifPresent(user -> user.setLastLogin(new Date()));
  }

  @Override
  public void delete(GbifUser gbifUser) {
    Optional.ofNullable(getByKey(gbifUser.getKey()))
        .ifPresent(user -> users.remove(user.getUserName()));
  }

  @Override
  public void deleteByKey(int key) {
    Optional.ofNullable(getByKey(key)).ifPresent(user -> users.remove(user.getUserName()));
  }

  @Override
  public void update(GbifUser gbifUser) {
    users.put(gbifUser.getUserName(), gbifUser);
  }

  @Override
  public List<UUID> listEditorRights(String user) {
    return null;
  }

  @Override
  public void addEditorRight(String userName, UUID uuid) {}

  @Override
  public void deleteEditorRight(String userName, UUID uuid) {}

  @Override
  public Integer getChallengeCodeKey(Integer key) {
    return null;
  }

  @Override
  public boolean updateChallengeCodeKey(Integer key, Integer challengeCodeKey) {
    return false;
  }

  @Override
  public List<GbifUser> search(
      @Nullable String s,
      @Nullable Set<UserRole> set,
      @Nullable Set<UUID> set1,
      @Nullable Set<String> set2,
      @Nullable Set<Country> set3,
      @Nullable Pageable pageable) {
    Stream<GbifUser> userStream = users.values().stream();
    if (Objects.nonNull(pageable)) {
      userStream = userStream.skip(pageable.getOffset()).limit(pageable.getLimit());
    }
    return userStream.collect(Collectors.toList());
  }

  @Override
  public int count(
      @Nullable String s,
      @Nullable Set<UserRole> set,
      @Nullable Set<UUID> set1,
      @Nullable Set<String> set2,
      @Nullable Set<Country> set3) {
    return users.size();
  }

  @Override
  public void addNamespaceRight(String s, String s1) {}

  @Override
  public void deleteNamespaceRight(String s, String s1) {}

  @Override
  public List<String> listNamespaceRights(String s) {
    return null;
  }

  @Override
  public void addCountryRight(String s, Country country) {}

  @Override
  public void deleteCountryRight(String s, Country country) {}

  @Override
  public List<Country> listCountryRights(String s) {
    return null;
  }
}
