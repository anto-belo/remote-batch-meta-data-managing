package com.restbatch.batchservice.client;

import com.restbatch.batchservice.dto.holder.StepExecutionHolder;
import java.util.List;
import org.springframework.cloud.openfeign.FeignClient;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestParam;

/**
 * @author Anton Belousov
 * @since 0.0.1-SNAPSHOT
 */
@FeignClient(name = "stepExecDaoClient", url = "${api.base-url}/dao/stepExec")
public interface StepExecutionDaoClient {

  @PostMapping("/saveStepExecution")
  void saveStepExecution(@RequestBody StepExecutionHolder holder);

  @PostMapping("/saveStepExecutions")
  void saveStepExecutions(@RequestBody List<StepExecutionHolder> stepExecutionHolders);

  @PostMapping("/updateStepExecution")
  Integer updateStepExecution(@RequestBody StepExecutionHolder holder);

  @GetMapping("/getStepExecutionVersion")
  Integer getStepExecutionVersion(@RequestParam Long stepExecutionId);

  @GetMapping("/getStepExecution")
  List<StepExecutionHolder> getStepExecution(@RequestParam Long jobExecutionId,
      @RequestParam Long stepExecutionId);

  @GetMapping("/getLastStepExecution")
  StepExecutionHolder getLastStepExecution(@RequestParam Long jobInstanceId,
      @RequestParam String stepName);

  @GetMapping("/getStepExecutions")
  List<StepExecutionHolder> getStepExecutions(@RequestParam Long jobExecutionId);

  @GetMapping("/countStepExecutions")
  Integer countStepExecutions(@RequestParam Long jobInstanceId,
      @RequestParam String stepName);

  @GetMapping("/nextStepExecutionId")
  Long nextStepExecutionId();
}
