package com.restdatabase.databaseservice.service;

import com.restdatabase.databaseservice.dto.SerializedContextDto;
import com.restdatabase.databaseservice.dto.SerializedContextsDto;
import org.springframework.batch.item.ExecutionContext;

/**
 * @author Anton Belousov
 * @since 0.0.1-SNAPSHOT
 */
public interface ExecutionContextService {

  ExecutionContext getJobExecutionContext(Long executionId);

  ExecutionContext getStepExecutionContext(Long executionId);

  void persistSerializedContext(SerializedContextDto ctxDto);

  void persistSerializedContexts(SerializedContextsDto ctxDto);
}
