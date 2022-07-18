package com.restdatabase.databaseservice.dto.holder;

import com.fasterxml.jackson.annotation.JsonIgnore;
import java.util.Date;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.springframework.batch.core.BatchStatus;
import org.springframework.batch.core.ExitStatus;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.StepExecution;
import org.springframework.util.Assert;

/**
 * @author Anton Belousov
 * @since 0.0.1-SNAPSHOT
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class StepExecutionHolder {

  private Long id;
  private String stepName;
  private JobExecution jobExecution;
  private Long jobExecutionId;
  private Date startTime;
  private Date endTime;
  private BatchStatus status;
  private int commitCount;
  private int readCount;
  private int filterCount;
  private int writeCount;
  private String exitCode;
  private String exitDescription;
  private int readSkipCount;
  private int writeSkipCount;
  private int processSkipCount;
  private int rollbackCount;
  private Date lastUpdated;
  private Integer version;

  private Integer updateVersion;

  @JsonIgnore
  public StepExecution getTarget() {
    Assert.notNull(jobExecution, "Job execution must be not null.");

    StepExecution stepExecution = new StepExecution(stepName, jobExecution, id);
    stepExecution.setStartTime(startTime);
    stepExecution.setEndTime(endTime);
    stepExecution.setStatus(status);
    stepExecution.setCommitCount(commitCount);
    stepExecution.setReadCount(readCount);
    stepExecution.setFilterCount(filterCount);
    stepExecution.setWriteCount(writeCount);
    stepExecution.setExitStatus(new ExitStatus(exitCode, exitDescription));
    stepExecution.setReadSkipCount(readSkipCount);
    stepExecution.setWriteSkipCount(writeSkipCount);
    stepExecution.setProcessSkipCount(processSkipCount);
    stepExecution.setRollbackCount(rollbackCount);
    stepExecution.setLastUpdated(lastUpdated);
    stepExecution.setVersion(version);
    return stepExecution;
  }
}
