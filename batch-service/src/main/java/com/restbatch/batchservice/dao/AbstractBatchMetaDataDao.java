package com.restbatch.batchservice.dao;

import java.sql.Types;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.util.StringUtils;

/**
 * @author Anton Belousov
 * @since 0.0.1-SNAPSHOT
 */
public abstract class AbstractBatchMetaDataDao implements InitializingBean {

  /**
   * Default value for the table prefix property.
   */
  public static final String DEFAULT_TABLE_PREFIX = "BATCH_";

  public static final int DEFAULT_EXIT_MESSAGE_LENGTH = 2500;

  private String tablePrefix = DEFAULT_TABLE_PREFIX;

  private int clobTypeToUse = Types.CLOB;

  protected String getQuery(String base) {
    return StringUtils.replace(base, "%PREFIX%", tablePrefix);
  }

  protected String getTablePrefix() {
    return tablePrefix;
  }

  /**
   * Public setter for the table prefix property. This will be prefixed to all the table names
   * before queries are executed. Defaults to {@link #DEFAULT_TABLE_PREFIX}.
   *
   * @param tablePrefix the tablePrefix to set
   */
  public void setTablePrefix(String tablePrefix) {
    this.tablePrefix = tablePrefix;
  }

  public int getClobTypeToUse() {
    return clobTypeToUse;
  }

  public void setClobTypeToUse(int clobTypeToUse) {
    this.clobTypeToUse = clobTypeToUse;
  }
}
