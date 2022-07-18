package com.restdatabase.databaseservice.service.impl;

import com.restdatabase.databaseservice.service.ExecutionContextService;
import com.restdatabase.databaseservice.dto.SerializedContextDto;
import com.restdatabase.databaseservice.dto.SerializedContextsDto;
import com.restdatabase.databaseservice.service.DaoUtils;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import lombok.RequiredArgsConstructor;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.jdbc.core.BatchPreparedStatementSetter;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.support.lob.DefaultLobHandler;
import org.springframework.jdbc.support.lob.LobHandler;
import org.springframework.stereotype.Service;
import org.springframework.util.Assert;

/**
 * @author Anton Belousov
 * @since 0.0.1-SNAPSHOT
 */
@Service
@RequiredArgsConstructor
public class ExecutionContextServiceImpl implements ExecutionContextService {

  private static final String FIND_JOB_EXECUTION_CONTEXT = "SELECT SHORT_CONTEXT, SERIALIZED_CONTEXT "
      + "FROM %PREFIX%JOB_EXECUTION_CONTEXT WHERE JOB_EXECUTION_ID = ?";

  private static final String INSERT_JOB_EXECUTION_CONTEXT = "INSERT INTO %PREFIX%JOB_EXECUTION_CONTEXT "
      + "(SHORT_CONTEXT, SERIALIZED_CONTEXT, JOB_EXECUTION_ID) " + "VALUES(?, ?, ?)";

  private static final String UPDATE_JOB_EXECUTION_CONTEXT = "UPDATE %PREFIX%JOB_EXECUTION_CONTEXT "
      + "SET SHORT_CONTEXT = ?, SERIALIZED_CONTEXT = ? " + "WHERE JOB_EXECUTION_ID = ?";

  private static final String FIND_STEP_EXECUTION_CONTEXT = "SELECT SHORT_CONTEXT, SERIALIZED_CONTEXT "
      + "FROM %PREFIX%STEP_EXECUTION_CONTEXT WHERE STEP_EXECUTION_ID = ?";

  private static final String INSERT_STEP_EXECUTION_CONTEXT = "INSERT INTO %PREFIX%STEP_EXECUTION_CONTEXT "
      + "(SHORT_CONTEXT, SERIALIZED_CONTEXT, STEP_EXECUTION_ID) " + "VALUES(?, ?, ?)";

  private static final String UPDATE_STEP_EXECUTION_CONTEXT = "UPDATE %PREFIX%STEP_EXECUTION_CONTEXT "
      + "SET SHORT_CONTEXT = ?, SERIALIZED_CONTEXT = ? " + "WHERE STEP_EXECUTION_ID = ?";

  private final JdbcTemplate jdbcTemplate;
  private final LobHandler lobHandler = new DefaultLobHandler();

  @Value("${table.prefix}")
  private String tablePrefix;

  @Override
  public ExecutionContext getJobExecutionContext(Long executionId) {
    List<ExecutionContext> results = jdbcTemplate.query(
        DaoUtils.getQuery(FIND_JOB_EXECUTION_CONTEXT, tablePrefix),
        new ExecutionContextRowMapper(), executionId);
    if (results.size() > 0) {
      return results.get(0);
    } else {
      return new ExecutionContext();
    }
  }

  @Override
  public ExecutionContext getStepExecutionContext(Long executionId) {
    List<ExecutionContext> results = jdbcTemplate.query(
        DaoUtils.getQuery(FIND_STEP_EXECUTION_CONTEXT, tablePrefix),
        new ExecutionContextRowMapper(), executionId);
    if (results.size() > 0) {
      return results.get(0);
    } else {
      return new ExecutionContext();
    }
  }

  @Override
  public void persistSerializedContext(SerializedContextDto ctxDto) {
    String sql = getSql(ctxDto.getSqlType());
    Assert.notNull(sql, "Unknown sql type");

    jdbcTemplate.update(DaoUtils.getQuery(sql, tablePrefix), ps -> {
      ps.setString(1, ctxDto.getShortContext());
      if (ctxDto.getLongContext() != null) {
        lobHandler.getLobCreator().setClobAsString(ps, 2, ctxDto.getLongContext());
      } else {
        ps.setNull(2, ctxDto.getClobTypeToUse());
      }
      ps.setLong(3, ctxDto.getExecutionId());
    });
  }

  @Override
  public void persistSerializedContexts(SerializedContextsDto ctxDto) {
    Map<Long, String> serializedContexts = ctxDto.getSerializedContexts();
    if (!serializedContexts.isEmpty()) {
      String sql = getSql(ctxDto.getSqlType());
      Assert.notNull(sql, "Unknown sql type");

      final Iterator<Long> executionIdIterator = serializedContexts.keySet().iterator();

      jdbcTemplate.batchUpdate(DaoUtils.getQuery(sql, tablePrefix), new BatchPreparedStatementSetter() {
        @Override
        public void setValues(PreparedStatement ps, int i) throws SQLException {
          Long executionId = executionIdIterator.next();
          String serializedContext = serializedContexts.get(executionId);
          String shortContext;
          String longContext;
          if (serializedContext.length() > ctxDto.getShortContextLength()) {
            // Overestimate length of ellipsis to be on the safe side with
            // 2-byte chars
            shortContext = serializedContext.substring(0, ctxDto.getShortContextLength() - 8) + " ...";
            longContext = serializedContext;
          } else {
            shortContext = serializedContext;
            longContext = null;
          }
          ps.setString(1, shortContext);
          if (longContext != null) {
            lobHandler.getLobCreator().setClobAsString(ps, 2, longContext);
          } else {
            ps.setNull(2, ctxDto.getClobTypeToUse());
          }
          ps.setLong(3, executionId);
        }

        @Override
        public int getBatchSize() {
          return serializedContexts.size();
        }
      });
    }
  }

  private String getSql(String sqlType) {
    switch (sqlType) {
      case "updateJobExecutionContext":
        return UPDATE_JOB_EXECUTION_CONTEXT;
      case "updateStepExecutionContext":
        return UPDATE_STEP_EXECUTION_CONTEXT;
      case "insertJobExecutionContext":
        return INSERT_JOB_EXECUTION_CONTEXT;
      case "insertStepExecutionContext":
        return INSERT_STEP_EXECUTION_CONTEXT;
      default:
        return null;
    }
  }

  private static class ExecutionContextRowMapper implements RowMapper<ExecutionContext> {

    @Override
    public ExecutionContext mapRow(ResultSet rs, int i) throws SQLException {
      ExecutionContext executionContext = new ExecutionContext();
      String serializedContext = rs.getString("SERIALIZED_CONTEXT");
      if (serializedContext == null) {
        serializedContext = rs.getString("SHORT_CONTEXT");
      }

      executionContext.put("_CTX", serializedContext);
      return executionContext;
    }
  }
}
