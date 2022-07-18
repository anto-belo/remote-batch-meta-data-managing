package com.restdatabase.databaseservice.service.impl;

import com.restdatabase.databaseservice.dto.holder.JobExecutionHolder;
import com.restdatabase.databaseservice.dto.holder.JobExecutionParamHolder;
import com.restdatabase.databaseservice.service.DaoUtils;
import com.restdatabase.databaseservice.service.JobExecutionService;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import lombok.RequiredArgsConstructor;
import org.springframework.batch.core.BatchStatus;
import org.springframework.batch.core.ExitStatus;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobParameter;
import org.springframework.batch.core.JobParameter.ParameterType;
import org.springframework.batch.core.JobParameters;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.dao.EmptyResultDataAccessException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowCallbackHandler;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.support.incrementer.DataFieldMaxValueIncrementer;
import org.springframework.stereotype.Service;

/**
 * @author Anton Belousov
 * @since 0.0.1-SNAPSHOT
 */
@Service
@RequiredArgsConstructor
public class JobExecutionServiceImpl implements JobExecutionService {

  private static final String SAVE_JOB_EXECUTION =
      "INSERT into %PREFIX%JOB_EXECUTION(JOB_EXECUTION_ID, JOB_INSTANCE_ID, START_TIME, "
          + "END_TIME, STATUS, EXIT_CODE, EXIT_MESSAGE, VERSION, CREATE_TIME, LAST_UPDATED, JOB_CONFIGURATION_LOCATION) values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";

  private static final String CHECK_JOB_EXECUTION_EXISTS = "SELECT COUNT(*) FROM %PREFIX%JOB_EXECUTION WHERE JOB_EXECUTION_ID = ?";

  private static final String GET_STATUS = "SELECT STATUS from %PREFIX%JOB_EXECUTION where JOB_EXECUTION_ID = ?";

  private static final String UPDATE_JOB_EXECUTION =
      "UPDATE %PREFIX%JOB_EXECUTION set START_TIME = ?, END_TIME = ?, "
          + " STATUS = ?, EXIT_CODE = ?, EXIT_MESSAGE = ?, VERSION = ?, CREATE_TIME = ?, LAST_UPDATED = ? where JOB_EXECUTION_ID = ? and VERSION = ?";

  private static final String FIND_JOB_EXECUTIONS =
      "SELECT JOB_EXECUTION_ID, START_TIME, END_TIME, STATUS, EXIT_CODE, EXIT_MESSAGE, CREATE_TIME, LAST_UPDATED, VERSION, JOB_CONFIGURATION_LOCATION"
          + " from %PREFIX%JOB_EXECUTION where JOB_INSTANCE_ID = ? order by JOB_EXECUTION_ID desc";

  private static final String GET_LAST_EXECUTION =
      "SELECT JOB_EXECUTION_ID, START_TIME, END_TIME, STATUS, EXIT_CODE, EXIT_MESSAGE, CREATE_TIME, LAST_UPDATED, VERSION, JOB_CONFIGURATION_LOCATION "
          + "from %PREFIX%JOB_EXECUTION E where JOB_INSTANCE_ID = ? and JOB_EXECUTION_ID in (SELECT max(JOB_EXECUTION_ID) from %PREFIX%JOB_EXECUTION E2 where E2.JOB_INSTANCE_ID = ?)";

  private static final String GET_EXECUTION_BY_ID =
      "SELECT JOB_EXECUTION_ID, START_TIME, END_TIME, STATUS, EXIT_CODE, EXIT_MESSAGE, CREATE_TIME, LAST_UPDATED, VERSION, JOB_CONFIGURATION_LOCATION"
          + " from %PREFIX%JOB_EXECUTION where JOB_EXECUTION_ID = ?";

  private static final String GET_RUNNING_EXECUTIONS =
      "SELECT E.JOB_EXECUTION_ID, E.START_TIME, E.END_TIME, E.STATUS, E.EXIT_CODE, E.EXIT_MESSAGE, E.CREATE_TIME, E.LAST_UPDATED, E.VERSION, "
          + "E.JOB_INSTANCE_ID, E.JOB_CONFIGURATION_LOCATION from %PREFIX%JOB_EXECUTION E, %PREFIX%JOB_INSTANCE I where E.JOB_INSTANCE_ID=I.JOB_INSTANCE_ID and I.JOB_NAME=? and E.START_TIME is not NULL and E.END_TIME is NULL order by E.JOB_EXECUTION_ID desc";

  private static final String CURRENT_VERSION_JOB_EXECUTION = "SELECT VERSION FROM %PREFIX%JOB_EXECUTION WHERE JOB_EXECUTION_ID=?";

  private static final String FIND_PARAMS_FROM_ID = "SELECT JOB_EXECUTION_ID, KEY_NAME, TYPE_CD, "
      + "STRING_VAL, DATE_VAL, LONG_VAL, DOUBLE_VAL, IDENTIFYING from %PREFIX%JOB_EXECUTION_PARAMS where JOB_EXECUTION_ID = ?";

  private static final String CREATE_JOB_PARAMETERS =
      "INSERT into %PREFIX%JOB_EXECUTION_PARAMS(JOB_EXECUTION_ID, KEY_NAME, TYPE_CD, "
          + "STRING_VAL, DATE_VAL, LONG_VAL, DOUBLE_VAL, IDENTIFYING) values (?, ?, ?, ?, ?, ?, ?, ?)";

  private final JdbcTemplate jdbcTemplate;
  private final DataFieldMaxValueIncrementer jobExecutionIncrementer;

  @Value("${table.prefix}")
  private String tablePrefix;

  @Override
  public List<JobExecution> findJobExecutions(Long jobInstanceId) {
    return jdbcTemplate.query(DaoUtils.getQuery(FIND_JOB_EXECUTIONS, tablePrefix),
        new JobExecutionRowMapper(), jobInstanceId);
  }

  @Override
  public Long saveJobExecution(JobExecutionHolder holder) {
    Long jobExecutionId = jobExecutionIncrementer.nextLongValue();
    Object[] parameters = new Object[]{jobExecutionId, holder.getJobId(), holder.getStartTime(),
        holder.getEndTime(), holder.getStatus(), holder.getExitCode(), holder.getExitDescription(),
        holder.getVersion(), holder.getCreateTime(), holder.getLastUpdated(),
        holder.getJobConfigurationName()};

    jdbcTemplate.update(
        DaoUtils.getQuery(SAVE_JOB_EXECUTION, tablePrefix), parameters,
        new int[]{Types.BIGINT, Types.BIGINT, Types.TIMESTAMP, Types.TIMESTAMP, Types.VARCHAR,
            Types.VARCHAR, Types.VARCHAR, Types.INTEGER, Types.TIMESTAMP, Types.TIMESTAMP,
            Types.VARCHAR});
    return jobExecutionId;
  }

  @Override
  public Integer countJobExecutions(Long jobExecutionId) {
    return jdbcTemplate.queryForObject(DaoUtils.getQuery(CHECK_JOB_EXECUTION_EXISTS, tablePrefix),
        Integer.class, jobExecutionId);
  }

  @Override
  public Integer updateJobExecution(JobExecutionHolder holder) {
    Object[] parameters = new Object[]{holder.getStartTime(), holder.getEndTime(),
        holder.getStatus(), holder.getExitCode(), holder.getExitDescription(),
        holder.getUpdateVersion(), holder.getCreateTime(), holder.getLastUpdated(), holder.getId(),
        holder.getVersion()};

    return jdbcTemplate.update(
        DaoUtils.getQuery(UPDATE_JOB_EXECUTION, tablePrefix), parameters,
        new int[]{Types.TIMESTAMP, Types.TIMESTAMP, Types.VARCHAR, Types.VARCHAR, Types.VARCHAR,
            Types.INTEGER, Types.TIMESTAMP, Types.TIMESTAMP, Types.BIGINT, Types.INTEGER});
  }

  @Override
  public Integer getJobExecutionVersion(Long jobExecutionId) {
    return jdbcTemplate.queryForObject(
        DaoUtils.getQuery(CURRENT_VERSION_JOB_EXECUTION, tablePrefix), Integer.class,
        jobExecutionId);
  }

  @Override
  public String getJobExecutionStatus(Long jobExecutionId) {
    return jdbcTemplate.queryForObject(
        DaoUtils.getQuery(GET_STATUS, tablePrefix), String.class, jobExecutionId);
  }

  @Override
  public List<JobExecution> getLastJobExecution(Long jobInstanceId) {
    return jdbcTemplate.query(
        DaoUtils.getQuery(GET_LAST_EXECUTION, tablePrefix), new JobExecutionRowMapper(),
        jobInstanceId, jobInstanceId);
  }

  @Override
  public JobExecution getJobExecution(Long executionId) {
    try {
      return jdbcTemplate.queryForObject(DaoUtils.getQuery(GET_EXECUTION_BY_ID, tablePrefix),
          new JobExecutionRowMapper(), executionId);
    } catch (EmptyResultDataAccessException e) {
      return null;
    }
  }

  @Override
  public Set<JobExecution> findRunningJobExecutions(String jobName) {
    final Set<JobExecution> result = new HashSet<>();
    RowCallbackHandler handler = rs -> {
      JobExecutionRowMapper mapper = new JobExecutionRowMapper();
      result.add(mapper.mapRow(rs, 0));
    };
    jdbcTemplate.query(DaoUtils.getQuery(GET_RUNNING_EXECUTIONS, tablePrefix), handler, jobName);

    return result;
  }

  @Override
  public void createJobParameters(JobExecutionParamHolder holder) {
    Object[] args = new Object[]{holder.getJobExecutionId(), holder.getKeyName(), holder.getType(),
        holder.getStringValue(), holder.getDateValue(), holder.getLongValue(),
        holder.getDoubleValue(), holder.getIdentifyingFlag()};

    jdbcTemplate.update(DaoUtils.getQuery(CREATE_JOB_PARAMETERS, tablePrefix), args,
        new int[]{Types.BIGINT, Types.VARCHAR, Types.VARCHAR, Types.VARCHAR, Types.TIMESTAMP,
            Types.BIGINT, Types.DOUBLE, Types.CHAR});
  }

  /**
   * Re-usable mapper for {@link JobExecution} instances.
   *
   * @author Dave Syer
   */
  private final class JobExecutionRowMapper implements RowMapper<JobExecution> {

    private JobParameters jobParameters;

    public JobExecutionRowMapper() {
    }

    /**
     * @param executionId {@link Long} containing the id for the execution.
     * @return job parameters for the requested execution id
     */
    private JobParameters getJobParameters(Long executionId) {
      final Map<String, JobParameter> map = new HashMap<>();
      RowCallbackHandler handler = rs -> {
        ParameterType type = ParameterType.valueOf(rs.getString(3));
        JobParameter value = null;

        if (type == ParameterType.STRING) {
          value = new JobParameter(rs.getString(4), rs.getString(8).equalsIgnoreCase("Y"));
        } else if (type == ParameterType.LONG) {
          long longValue = rs.getLong(6);
          value = new JobParameter(rs.wasNull() ? null : longValue,
              rs.getString(8).equalsIgnoreCase("Y"));
        } else if (type == ParameterType.DOUBLE) {
          double doubleValue = rs.getDouble(7);
          value = new JobParameter(rs.wasNull() ? null : doubleValue,
              rs.getString(8).equalsIgnoreCase("Y"));
        } else if (type == ParameterType.DATE) {
          value = new JobParameter(rs.getTimestamp(5), rs.getString(8).equalsIgnoreCase("Y"));
        }

        // No need to assert that value is not null because it's an enum
        map.put(rs.getString(2), value);
      };

      jdbcTemplate.query(DaoUtils.getQuery(FIND_PARAMS_FROM_ID, tablePrefix), handler, executionId);

      return new JobParameters(map);
    }

    @Override
    public JobExecution mapRow(ResultSet rs, int rowNum) throws SQLException {
      Long id = rs.getLong(1);
      String jobConfigurationLocation = rs.getString(10);
      JobExecution jobExecution;
      if (jobParameters == null) {
        jobParameters = getJobParameters(id);
      }

      jobExecution = new JobExecution(id, jobParameters, jobConfigurationLocation);

      jobExecution.setStartTime(rs.getTimestamp(2));
      jobExecution.setEndTime(rs.getTimestamp(3));
      jobExecution.setStatus(BatchStatus.valueOf(rs.getString(4)));
      jobExecution.setExitStatus(new ExitStatus(rs.getString(5), rs.getString(6)));
      jobExecution.setCreateTime(rs.getTimestamp(7));
      jobExecution.setLastUpdated(rs.getTimestamp(8));
      jobExecution.setVersion(rs.getInt(9));
      return jobExecution;
    }
  }
}
