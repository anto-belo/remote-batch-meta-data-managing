package com.restdatabase.databaseservice.service.impl;

import com.restdatabase.databaseservice.service.JobInstanceService;
import com.restdatabase.databaseservice.service.DaoUtils;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.List;
import lombok.RequiredArgsConstructor;
import org.springframework.batch.core.JobInstance;
import org.springframework.batch.core.launch.NoSuchJobException;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.dao.DataAccessException;
import org.springframework.dao.EmptyResultDataAccessException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.ResultSetExtractor;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.support.incrementer.DataFieldMaxValueIncrementer;
import org.springframework.stereotype.Service;
import org.springframework.util.StringUtils;

/**
 * @author Anton Belousov
 * @since 0.0.1-SNAPSHOT
 */
@Service
@RequiredArgsConstructor
public class JobInstanceServiceImpl implements JobInstanceService {

  private static final String STAR_WILDCARD = "*";

  private static final String SQL_WILDCARD = "%";

  private static final String CREATE_JOB_INSTANCE = "INSERT into %PREFIX%JOB_INSTANCE(JOB_INSTANCE_ID, JOB_NAME, JOB_KEY, VERSION)"
      + " values (?, ?, ?, ?)";

  private static final String FIND_JOBS_WITH_NAME = "SELECT JOB_INSTANCE_ID, JOB_NAME from %PREFIX%JOB_INSTANCE where JOB_NAME = ?";

  private static final String FIND_JOBS_WITH_KEY = FIND_JOBS_WITH_NAME
      + " and JOB_KEY = ?";

  private static final String COUNT_JOBS_WITH_NAME = "SELECT COUNT(*) from %PREFIX%JOB_INSTANCE where JOB_NAME = ?";

  private static final String FIND_JOBS_WITH_EMPTY_KEY = "SELECT JOB_INSTANCE_ID, JOB_NAME from %PREFIX%JOB_INSTANCE where JOB_NAME = ? and (JOB_KEY = ? OR JOB_KEY is NULL)";

  private static final String GET_JOB_FROM_ID = "SELECT JOB_INSTANCE_ID, JOB_NAME, JOB_KEY, VERSION from %PREFIX%JOB_INSTANCE where JOB_INSTANCE_ID = ?";

  private static final String GET_JOB_FROM_EXECUTION_ID = "SELECT ji.JOB_INSTANCE_ID, JOB_NAME, JOB_KEY, ji.VERSION from %PREFIX%JOB_INSTANCE ji, "
      + "%PREFIX%JOB_EXECUTION je where JOB_EXECUTION_ID = ? and ji.JOB_INSTANCE_ID = je.JOB_INSTANCE_ID";

  private static final String FIND_JOB_NAMES = "SELECT distinct JOB_NAME from %PREFIX%JOB_INSTANCE order by JOB_NAME";

  private static final String FIND_LAST_JOBS_BY_NAME = "SELECT JOB_INSTANCE_ID, JOB_NAME from %PREFIX%JOB_INSTANCE where JOB_NAME = ? order by JOB_INSTANCE_ID desc";

  private static final String FIND_LAST_JOB_INSTANCE_BY_JOB_NAME = "SELECT JOB_INSTANCE_ID, JOB_NAME from %PREFIX%JOB_INSTANCE I1 where" +
      " I1.JOB_NAME = ? and I1.JOB_INSTANCE_ID in (SELECT max(I2.JOB_INSTANCE_ID) from %PREFIX%JOB_INSTANCE I2 where I2.JOB_NAME = ?)";

  private static final String FIND_LAST_JOBS_LIKE_NAME = "SELECT JOB_INSTANCE_ID, JOB_NAME from %PREFIX%JOB_INSTANCE where JOB_NAME like ? order by JOB_INSTANCE_ID desc";

  private final JdbcTemplate jdbcTemplate;
  private final DataFieldMaxValueIncrementer jobInstanceIncrementer;

  @Value("${table.prefix}")
  private String tablePrefix;

  @Override
  public Long createJobInstance(Object[] args) {
    Long jobId = jobInstanceIncrementer.nextLongValue();
    List<Object> argsList = new ArrayList<>(List.of(args));
    argsList.add(0, jobId);
    jdbcTemplate.update(
        DaoUtils.getQuery(CREATE_JOB_INSTANCE, tablePrefix),
        argsList.toArray(),
        new int[]{Types.BIGINT, Types.VARCHAR, Types.VARCHAR, Types.INTEGER});
    return jobId;
  }

  @Override
  public List<JobInstance> getJobInstance(String jobName, String jobKey) {
    RowMapper<JobInstance> rowMapper = new JobInstanceRowMapper();
    List<JobInstance> instances;
    if (StringUtils.hasLength(jobKey)) {
      instances = jdbcTemplate.query(DaoUtils.getQuery(FIND_JOBS_WITH_KEY, tablePrefix),
          rowMapper, jobName, jobKey);
    } else {
      instances = jdbcTemplate.query(DaoUtils.getQuery(FIND_JOBS_WITH_EMPTY_KEY, tablePrefix),
          rowMapper, jobName, jobKey);
    }
    return instances;
  }

  @Override
  public JobInstance getJobInstance(Long instanceId) {
    try {
      return jdbcTemplate.queryForObject(DaoUtils.getQuery(GET_JOB_FROM_ID, tablePrefix),
          new JobInstanceRowMapper(), instanceId);
    } catch (EmptyResultDataAccessException e) {
      return null;
    }
  }

  @Override
  public List<String> getJobNames() {
    return jdbcTemplate.query(DaoUtils.getQuery(FIND_JOB_NAMES, tablePrefix),
        (rs, rowNum) -> rs.getString(1));
  }

  @Override
  public List<JobInstance> getJobInstances(String jobName, int start, int count) {
    ResultSetExtractor<List<JobInstance>> extractor = new ResultSetExtractor<>() {

      private final List<JobInstance> list = new ArrayList<>();

      @Override
      public List<JobInstance> extractData(ResultSet rs) throws SQLException,
          DataAccessException {
        int rowNum = 0;
        while (rowNum < start && rs.next()) {
          rowNum++;
        }
        while (rowNum < start + count && rs.next()) {
          RowMapper<JobInstance> rowMapper = new JobInstanceRowMapper();
          list.add(rowMapper.mapRow(rs, rowNum));
          rowNum++;
        }
        return list;
      }

    };

    return jdbcTemplate.query(DaoUtils.getQuery(FIND_LAST_JOBS_BY_NAME, tablePrefix),
        new Object[]{jobName}, new int[]{Types.VARCHAR}, extractor);
  }

  @Override
  public JobInstance getLastJobInstance(String jobName) {
    try {
      return jdbcTemplate.queryForObject(
          DaoUtils.getQuery(FIND_LAST_JOB_INSTANCE_BY_JOB_NAME, tablePrefix),
          new Object[]{jobName, jobName}, new int[]{Types.VARCHAR, Types.VARCHAR},
          new JobInstanceRowMapper());
    } catch (EmptyResultDataAccessException e) {
      return null;
    }
  }

  @Override
  public JobInstance getJobInstanceByJobExecutionId(Long jobExecutionId) {
    try {
      return jdbcTemplate.queryForObject(
          DaoUtils.getQuery(GET_JOB_FROM_EXECUTION_ID, tablePrefix),
          new JobInstanceRowMapper(), jobExecutionId);
    } catch (EmptyResultDataAccessException e) {
      return null;
    }
  }

  @Override
  public Object getJobInstanceCount(String jobName) {
    try {
      return jdbcTemplate.queryForObject(
          DaoUtils.getQuery(COUNT_JOBS_WITH_NAME, tablePrefix), Integer.class, jobName);
    } catch (EmptyResultDataAccessException e) {
      return new NoSuchJobException("No job instances were found for job name " + jobName);
    }
  }

  @Override
  public List<JobInstance> findJobInstancesByName(String jobName, int start, int count) {
    @SuppressWarnings("rawtypes")
    ResultSetExtractor extractor = new ResultSetExtractor() {
      private final List<JobInstance> list = new ArrayList<>();

      @Override
      public Object extractData(ResultSet rs) throws SQLException,
          DataAccessException {
        int rowNum = 0;
        while (rowNum < start && rs.next()) {
          rowNum++;
        }
        while (rowNum < start + count && rs.next()) {
          RowMapper<JobInstance> rowMapper = new JobInstanceRowMapper();
          list.add(rowMapper.mapRow(rs, rowNum));
          rowNum++;
        }
        return list;
      }
    };

    if (jobName.contains(STAR_WILDCARD)) {
      jobName = jobName.replaceAll("\\" + STAR_WILDCARD, SQL_WILDCARD);
    }

    @SuppressWarnings("unchecked")
    List<JobInstance> result = (List<JobInstance>) jdbcTemplate.query(
        DaoUtils.getQuery(FIND_LAST_JOBS_LIKE_NAME, tablePrefix),
        new Object[]{jobName}, new int[]{Types.VARCHAR}, extractor);

    return result;
  }

  /**
   * @author Dave Syer
   */
  private static final class JobInstanceRowMapper implements RowMapper<JobInstance> {

    public JobInstanceRowMapper() {
    }

    @Override
    public JobInstance mapRow(ResultSet rs, int rowNum) throws SQLException {
      JobInstance jobInstance = new JobInstance(rs.getLong(1), rs.getString(2));
      // should always be at version=0 because they never get updated
      jobInstance.incrementVersion();
      return jobInstance;
    }
  }
}
