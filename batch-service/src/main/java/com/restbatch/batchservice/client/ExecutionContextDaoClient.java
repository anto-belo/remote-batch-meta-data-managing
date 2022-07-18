package com.restbatch.batchservice.client;

import com.restbatch.batchservice.dto.SerializedContextDto;
import com.restbatch.batchservice.dto.SerializedContextsDto;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.cloud.openfeign.FeignClient;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestParam;

/**
 * @author Anton Belousov
 * @since 0.0.1-SNAPSHOT
 */
@FeignClient(name = "execCtxDaoClient", url = "${api.base-url}/dao/execCtx")
public interface ExecutionContextDaoClient {

  @GetMapping("/getJobExecutionContext")
  ExecutionContext getJobExecutionContext(@RequestParam Long executionId);

  @GetMapping("/getStepExecutionContext")
  ExecutionContext getStepExecutionContext(@RequestParam Long executionId);

  @PostMapping("/persistSerializedContext")
  void persistSerializedContext(@RequestBody SerializedContextDto ctxDto);

  @PostMapping("/persistSerializedContexts")
  void persistSerializedContexts(@RequestBody SerializedContextsDto ctxDto);
}
