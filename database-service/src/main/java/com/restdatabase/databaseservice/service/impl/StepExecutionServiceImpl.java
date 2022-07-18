package com.restdatabase.databaseservice.service.impl;

import com.restdatabase.databaseservice.dto.holder.StepExecutionHolder;
import com.restdatabase.databaseservice.service.DaoUtils;
import com.restdatabase.databaseservice.service.StepExecutionService;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import lombok.RequiredArgsConstructor;
import org.springframework.batch.core.BatchStatus;
import org.springframework.batch.core.ExitStatus;
import org.springframework.batch.core.JobExecution;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.jdbc.core.BatchPreparedStatementSetter;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.support.incrementer.DataFieldMaxValueIncrementer;
import org.springframework.stereotype.Service;

/**
 * @author Anton Belousov
 * @since 0.0.1-SNAPSHOT
 */
@Service
@RequiredArgsConstructor
public class StepExecutionServiceImpl implements StepExecutionService {

  private static final String SAVE_STEP_EXECUTION =
      "INSERT into %PREFIX%STEP_EXECUTION(STEP_EXECUTION_ID, VERSION, "
          + "STEP_NAME, JOB_EXECUTION_ID, START_TIME, END_TIME, STATUS, COMMIT_COUNT, READ_COUNT, FILTER_COUNT, "
          + "WRITE_COUNT, EXIT_CODE, EXIT_MESSAGE, READ_SKIP_COUNT, WRITE_SKIP_COUNT, PROCESS_SKIP_COUNT, "
          + "ROLLBACK_COUNT, LAST_UPDATED) values(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";

  private static final String UPDATE_STEP_EXECUTION =
      "UPDATE %PREFIX%STEP_EXECUTION set START_TIME = ?, END_TIME = ?, "
          + "STATUS = ?, COMMIT_COUNT = ?, READ_COUNT = ?, FILTER_COUNT = ?, WRITE_COUNT = ?, EXIT_CODE = ?, "
          + "EXIT_MESSAGE = ?, VERSION = ?, READ_SKIP_COUNT = ?, PROCESS_SKIP_COUNT = ?, WRITE_SKIP_COUNT = ?, "
          + "ROLLBACK_COUNT = ?, LAST_UPDATED = ?"
          + " where STEP_EXECUTION_ID = ? and VERSION = ?";

  private static final String GET_RAW_STEP_EXECUTIONS =
      "SELECT STEP_EXECUTION_ID, STEP_NAME, START_TIME, END_TIME, "
          + "STATUS, COMMIT_COUNT, READ_COUNT, FILTER_COUNT, WRITE_COUNT, EXIT_CODE, EXIT_MESSAGE, READ_SKIP_COUNT, "
          + "WRITE_SKIP_COUNT, PROCESS_SKIP_COUNT, ROLLBACK_COUNT, LAST_UPDATED, VERSION from %PREFIX%STEP_EXECUTION "
          + "where JOB_EXECUTION_ID = ?";

  private static final String GET_STEP_EXECUTIONS =
      GET_RAW_STEP_EXECUTIONS + " order by STEP_EXECUTION_ID";

  private static final String GET_STEP_EXECUTION =
      GET_RAW_STEP_EXECUTIONS + " and STEP_EXECUTION_ID = ?";

  private static final String GET_LAST_STEP_EXECUTION = "SELECT "
      + " SE.STEP_EXECUTION_ID, SE.STEP_NAME, SE.START_TIME, SE.END_TIME, SE.STATUS, SE.COMMIT_COUNT, "
      + "SE.READ_COUNT, SE.FILTER_COUNT, SE.WRITE_COUNT, SE.EXIT_CODE, SE.EXIT_MESSAGE, SE.READ_SKIP_COUNT, "
      + "SE.WRITE_SKIP_COUNT, SE.PROCESS_SKIP_COUNT, SE.ROLLBACK_COUNT, SE.LAST_UPDATED, SE.VERSION,"
      + " JE.JOB_EXECUTION_ID, JE.START_TIME, JE.END_TIME, JE.STATUS, JE.EXIT_CODE, JE.EXIT_MESSAGE, "
      + "JE.CREATE_TIME, JE.LAST_UPDATED, JE.VERSION"
      + " from %PREFIX%JOB_EXECUTION JE join %PREFIX%STEP_EXECUTION SE"
      + "      on SE.JOB_EXECUTION_ID = JE.JOB_EXECUTION_ID "
      + "where JE.JOB_INSTANCE_ID = ?"
      + "      and SE.STEP_NAME = ?"
      + " order by SE.START_TIME desc, SE.STEP_EXECUTION_ID desc";

  private static final String CURRENT_VERSION_STEP_EXECUTION =
      "SELECT VERSION FROM %PREFIX%STEP_EXECUTION WHERE "
          + "STEP_EXECUTION_ID=?";

  private static final String COUNT_STEP_EXECUTIONS = "SELECT COUNT(*) "
      + " from %PREFIX%JOB_EXECUTION JE JOIN %PREFIX%STEP_EXECUTION SE "
      + "      on SE.JOB_EXECUTION_ID = JE.JOB_EXECUTION_ID "
      + "where JE.JOB_INSTANCE_ID = ?"
      + "      and SE.STEP_NAME = ?";

  private final JdbcTemplate jdbcTemplate;
  private final DataFieldMaxValueIncrementer stepExecutionIncrementer;

  @Value("${table.prefix}")
  private String tablePrefix;

  @Override
  public void saveStepExecution(StepExecutionHolder holder) {
    Object[] parameters = new Object[]{holder.getId(), holder.getVersion(), holder.getStepName(),
        holder.getJobExecutionId(), holder.getStartTime(), holder.getEndTime(),
        holder.getStatus().toString(), holder.getCommitCount(), holder.getReadCount(),
        holder.getFilterCount(), holder.getWriteCount(), holder.getExitCode(),
        holder.getExitDescription(), holder.getReadSkipCount(), holder.getWriteSkipCount(),
        holder.getProcessSkipCount(), holder.getRollbackCount(), holder.getLastUpdated()};
    jdbcTemplate.update(DaoUtils.getQuery(SAVE_STEP_EXECUTION, tablePrefix), parameters,
        new int[]{Types.BIGINT, Types.INTEGER, Types.VARCHAR, Types.BIGINT, Types.TIMESTAMP,
            Types.TIMESTAMP, Types.VARCHAR, Types.INTEGER, Types.INTEGER, Types.INTEGER,
            Types.INTEGER, Types.VARCHAR, Types.VARCHAR, Types.INTEGER, Types.INTEGER,
            Types.INTEGER, Types.INTEGER, Types.TIMESTAMP});
  }

  @Override
  public void saveStepExecutions(List<StepExecutionHolder> stepExecutionHolders) {
    if (!stepExecutionHolders.isEmpty()) {
      final Iterator<StepExecutionHolder> iterator = stepExecutionHolders.iterator();
      jdbcTemplate.batchUpdate(DaoUtils.getQuery(SAVE_STEP_EXECUTION, tablePrefix),
          new BatchPreparedStatementSetter() {

            @Override
            public int getBatchSize() {
              return stepExecutionHolders.size();
            }

            @Override
            public void setValues(PreparedStatement ps, int i) throws SQLException {
              StepExecutionHolder holder = iterator.next();
              Object[] parameterValues = new Object[]{holder.getId(), holder.getVersion(),
                  holder.getStepName(), holder.getJobExecutionId(), holder.getStartTime(),
                  holder.getEndTime(), holder.getStatus().toString(), holder.getCommitCount(),
                  holder.getReadCount(), holder.getFilterCount(), holder.getWriteCount(),
                  holder.getExitCode(), holder.getExitDescription(), holder.getReadSkipCount(),
                  holder.getWriteSkipCount(), holder.getProcessSkipCount(),
                  holder.getRollbackCount(), holder.getLastUpdated()};
              Integer[] parameterTypes = new Integer[]{Types.BIGINT, Types.INTEGER, Types.VARCHAR,
                  Types.BIGINT, Types.TIMESTAMP, Types.TIMESTAMP, Types.VARCHAR, Types.INTEGER,
                  Types.INTEGER, Types.INTEGER, Types.INTEGER, Types.VARCHAR, Types.VARCHAR,
                  Types.INTEGER, Types.INTEGER, Types.INTEGER, Types.INTEGER, Types.TIMESTAMP};
              for (int index = 0; index < parameterValues.length; index++) {
                switch (parameterTypes[index]) {
                  case Types.INTEGER:
                    ps.setInt(index + 1, (Integer) parameterValues[index]);
                    break;
                  case Types.VARCHAR:
                    ps.setString(index + 1, (String) parameterValues[index]);
                    break;
                  case Types.TIMESTAMP:
                    if (parameterValues[index] != null) {
                      ps.setTimestamp(index + 1,
                          new Timestamp(((java.util.Date) parameterValues[index]).getTime()));
                    } else {
                      ps.setNull(index + 1, Types.TIMESTAMP);
                    }
                    break;
                  case Types.BIGINT:
                    ps.setLong(index + 1, (Long) parameterValues[index]);
                    break;
                  default:
                    throw new IllegalArgumentException(
                        "unsupported SQL parameter type for step execution field index " + i);
                }
              }
            }
          });
    }
  }

  @Override
  public Integer updateStepExecution(StepExecutionHolder holder) {
    Object[] parameters = new Object[]{holder.getStartTime(), holder.getEndTime(),
        holder.getStatus().toString(), holder.getCommitCount(), holder.getReadCount(),
        holder.getFilterCount(), holder.getWriteCount(), holder.getExitCode(),
        holder.getExitDescription(), holder.getUpdateVersion(), holder.getReadSkipCount(),
        holder.getProcessSkipCount(), holder.getWriteSkipCount(), holder.getRollbackCount(),
        holder.getLastUpdated(), holder.getId(), holder.getVersion()};
    return jdbcTemplate.update(DaoUtils.getQuery(UPDATE_STEP_EXECUTION, tablePrefix), parameters,
        new int[]{Types.TIMESTAMP, Types.TIMESTAMP, Types.VARCHAR, Types.INTEGER, Types.INTEGER,
            Types.INTEGER, Types.INTEGER, Types.VARCHAR, Types.VARCHAR, Types.INTEGER,
            Types.INTEGER, Types.INTEGER, Types.INTEGER, Types.INTEGER, Types.TIMESTAMP,
            Types.BIGINT, Types.INTEGER});
  }

  @Override
  public Integer getStepExecutionVersion(Long stepExecutionId) {
    return jdbcTemplate.queryForObject(
        DaoUtils.getQuery(CURRENT_VERSION_STEP_EXECUTION, tablePrefix), Integer.class,
        stepExecutionId);
  }

  @Override
  public List<StepExecutionHolder> getStepExecution(Long jobExecutionId, Long stepExecutionId) {
    return jdbcTemplate.query(DaoUtils.getQuery(GET_STEP_EXECUTION, tablePrefix),
        new StepExecutionRowMapper(), jobExecutionId, stepExecutionId);
  }

  @Override
  public StepExecutionHolder getLastStepExecution(Long jobInstanceId, String stepName) {
    StepExecutionRowMapper rowMapper = new StepExecutionRowMapper();
    List<StepExecutionHolder> executions = jdbcTemplate.query(
        DaoUtils.getQuery(GET_LAST_STEP_EXECUTION, tablePrefix),
        (rs, rowNum) -> {
          Long jobExecutionId = rs.getLong(18);
          JobExecution jobExecution = new JobExecution(jobExecutionId);
          jobExecution.setStartTime(rs.getTimestamp(19));
          jobExecution.setEndTime(rs.getTimestamp(20));
          jobExecution.setStatus(BatchStatus.valueOf(rs.getString(21)));
          jobExecution.setExitStatus(new ExitStatus(rs.getString(22), rs.getString(23)));
          jobExecution.setCreateTime(rs.getTimestamp(24));
          jobExecution.setLastUpdated(rs.getTimestamp(25));
          jobExecution.setVersion(rs.getInt(26));
          StepExecutionHolder holder = Optional.ofNullable(rowMapper.mapRow(rs, rowNum))
              .orElseThrow(() -> new RuntimeException("Can't build StepExecutionHolder"));
          holder.setJobExecution(jobExecution);
          return holder;
        },
        jobInstanceId, stepName);
    if (executions.isEmpty()) {
      return null;
    } else {
      return executions.get(0);
    }
  }

  @Override
  public List<StepExecutionHolder> getStepExecutions(Long jobExecutionId) {
    return jdbcTemplate.query(DaoUtils.getQuery(GET_STEP_EXECUTIONS, tablePrefix),
        new StepExecutionRowMapper(), jobExecutionId);
  }

  @Override
  public Integer countStepExecutions(Long jobInstanceId, String stepName) {
    return jdbcTemplate.queryForObject(DaoUtils.getQuery(COUNT_STEP_EXECUTIONS, tablePrefix),
        Integer.class, jobInstanceId, stepName);
  }

  @Override
  public Long nextStepExecutionId() {
    return stepExecutionIncrementer.nextLongValue();
  }

  private static class StepExecutionRowMapper implements RowMapper<StepExecutionHolder> {

    @Override
    public StepExecutionHolder mapRow(ResultSet rs, int rowNum) throws SQLException {
      return StepExecutionHolder.builder()
          .id(rs.getLong(1))
          .stepName(rs.getString(2))
          .startTime(rs.getTimestamp(3))
          .endTime(rs.getTimestamp(4))
          .status(BatchStatus.valueOf(rs.getString(5)))
          .commitCount(rs.getInt(6))
          .readCount(rs.getInt(7))
          .filterCount(rs.getInt(8))
          .writeCount(rs.getInt(9))
          .exitCode(rs.getString(10))
          .exitDescription(rs.getString(11))
          .readSkipCount(rs.getInt(12))
          .writeSkipCount(rs.getInt(13))
          .processSkipCount(rs.getInt(14))
          .rollbackCount(rs.getInt(15))
          .lastUpdated(rs.getTimestamp(16))
          .version(rs.getInt(17))
          .build();
    }
  }
}
