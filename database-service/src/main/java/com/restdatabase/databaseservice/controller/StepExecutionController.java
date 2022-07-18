package com.restdatabase.databaseservice.controller;

import com.restdatabase.databaseservice.dto.holder.StepExecutionHolder;
import com.restdatabase.databaseservice.service.StepExecutionService;
import java.util.List;
import lombok.RequiredArgsConstructor;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

/**
 * @author Anton Belousov
 * @since 0.0.1-SNAPSHOT
 */
@RestController
@RequiredArgsConstructor
@RequestMapping("/dao/stepExec")
public class StepExecutionController {

  private final StepExecutionService dao;

  @PostMapping("/saveStepExecution")
  public void saveStepExecution(@RequestBody StepExecutionHolder holder) {
    dao.saveStepExecution(holder);
  }

  @PostMapping("/saveStepExecutions")
  public void saveStepExecutions(@RequestBody List<StepExecutionHolder> stepExecutionHolders) {
    dao.saveStepExecutions(stepExecutionHolders);
  }

  @PostMapping("/updateStepExecution")
  public Integer updateStepExecution(@RequestBody StepExecutionHolder holder) {
    return dao.updateStepExecution(holder);
  }

  @GetMapping("/getStepExecutionVersion")
  public Integer getStepExecutionVersion(@RequestParam Long stepExecutionId) {
    return dao.getStepExecutionVersion(stepExecutionId);
  }

  @GetMapping("/getStepExecution")
  public List<StepExecutionHolder> getStepExecution(@RequestParam Long jobExecutionId,
      @RequestParam Long stepExecutionId) {
    return dao.getStepExecution(jobExecutionId, stepExecutionId);
  }

  @GetMapping("/getLastStepExecution")
  public StepExecutionHolder getLastStepExecution(@RequestParam Long jobInstanceId,
      @RequestParam String stepName) {
    return dao.getLastStepExecution(jobInstanceId, stepName);
  }

  @GetMapping("/getStepExecutions")
  public List<StepExecutionHolder> getStepExecutions(@RequestParam Long jobExecutionId) {
    return dao.getStepExecutions(jobExecutionId);
  }

  @GetMapping("/countStepExecutions")
  public Integer countStepExecutions(@RequestParam Long jobInstanceId,
      @RequestParam String stepName) {
    return dao.countStepExecutions(jobInstanceId, stepName);
  }

  @GetMapping("/nextStepExecutionId")
  public Long nextStepExecutionId() {
    return dao.nextStepExecutionId();
  }
}
