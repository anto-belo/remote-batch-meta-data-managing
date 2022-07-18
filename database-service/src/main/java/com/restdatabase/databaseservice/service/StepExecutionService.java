package com.restdatabase.databaseservice.service;

import com.restdatabase.databaseservice.dto.holder.StepExecutionHolder;
import java.util.List;

/**
 * @author Anton Belousov
 * @since 0.0.1-SNAPSHOT
 */
public interface StepExecutionService {

  void saveStepExecution(StepExecutionHolder holder);

  void saveStepExecutions(List<StepExecutionHolder> stepExecutionHolders);

  Integer updateStepExecution(StepExecutionHolder holder);

  Integer getStepExecutionVersion(Long stepExecutionId);

  List<StepExecutionHolder> getStepExecution(Long jobExecutionId, Long stepExecutionId);

  StepExecutionHolder getLastStepExecution(Long jobInstanceId, String stepName);

  List<StepExecutionHolder> getStepExecutions(Long jobExecutionId);

  Integer countStepExecutions(Long jobInstanceId, String stepName);

  Long nextStepExecutionId();
}
