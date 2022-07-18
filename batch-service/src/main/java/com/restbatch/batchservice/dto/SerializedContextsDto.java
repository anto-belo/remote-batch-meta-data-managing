package com.restbatch.batchservice.dto;

import java.util.Map;
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
public class SerializedContextsDto {

  private Map<Long, String> serializedContexts;
  private String sqlType;
  private Integer shortContextLength;
  private Integer clobTypeToUse;
}
