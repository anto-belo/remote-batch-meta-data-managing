package com.restdatabase.databaseservice.dto.holder;

import java.sql.Timestamp;
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
public class JobExecutionParamHolder {

  Long jobExecutionId;
  String keyName;
  String type;
  @Builder.Default
  String stringValue = "";
  @Builder.Default
  Timestamp dateValue = new Timestamp(0L);
  @Builder.Default
  Long longValue = 0L;
  @Builder.Default
  Double doubleValue = 0D;
  String identifyingFlag;
}
