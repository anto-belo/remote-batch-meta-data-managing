package com.restdatabase.databaseservice.controller;

import com.restdatabase.databaseservice.dto.SerializedContextDto;
import com.restdatabase.databaseservice.dto.SerializedContextsDto;
import com.restdatabase.databaseservice.service.ExecutionContextService;
import lombok.RequiredArgsConstructor;
import org.springframework.batch.item.ExecutionContext;
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
@RequestMapping("/dao/execCtx")
public class ExecutionContextController {

  private final ExecutionContextService dao;

  @GetMapping("/getJobExecutionContext")
  public ExecutionContext getJobExecutionContext(@RequestParam Long executionId) {
    return dao.getJobExecutionContext(executionId);
  }

  @GetMapping("/getStepExecutionContext")
  public ExecutionContext getStepExecutionContext(@RequestParam Long executionId) {
    return dao.getStepExecutionContext(executionId);
  }

  @PostMapping("/persistSerializedContext")
  public void persistSerializedContext(@RequestBody SerializedContextDto ctxDto) {
    dao.persistSerializedContext(ctxDto);
  }

  @PostMapping("/persistSerializedContexts")
  public void persistSerializedContexts(@RequestBody SerializedContextsDto ctxDto) {
    dao.persistSerializedContexts(ctxDto);
  }
}
