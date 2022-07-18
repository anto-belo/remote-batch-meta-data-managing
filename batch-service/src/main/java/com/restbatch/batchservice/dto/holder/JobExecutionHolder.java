package com.restbatch.batchservice.dto.holder;

import java.util.Date;
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
public class JobExecutionHolder {

  private Long id;
  private Long jobId;
  private Date startTime;
  private Date endTime;
  private String status;
  private String exitCode;
  private String exitDescription;
  private Integer version;
  private Date createTime;
  private Date lastUpdated;
  private String jobConfigurationName;

  private Integer updateVersion;
}
