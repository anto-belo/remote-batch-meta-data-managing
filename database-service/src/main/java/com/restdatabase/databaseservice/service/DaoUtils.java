package com.restdatabase.databaseservice.service;

import org.springframework.util.StringUtils;

/**
 * @author Anton Belousov
 * @since 0.0.1-SNAPSHOT
 */
public class DaoUtils {

  public static String getQuery(String base, String tablePrefix) {
    return StringUtils.replace(base, "%PREFIX%", tablePrefix);
  }
}
