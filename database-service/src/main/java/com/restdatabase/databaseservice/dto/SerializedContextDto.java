package com.restdatabase.databaseservice.dto;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @author Anton Belousov
 * @since 0.0.1-SNAPSHOT
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class SerializedContextDto {

  private Long executionId;
  private String shortContext;
  private String longContext;
  private String sqlType;
  private Integer clobTypeToUse;
}
