package com.restdatabase.databaseservice.config;

import com.fasterxml.jackson.databind.Module;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.restdatabase.databaseservice.deserializer.TransactionDefinitionDeserializer;
import java.sql.SQLException;
import javax.sql.DataSource;
import lombok.RequiredArgsConstructor;
import org.springframework.batch.item.database.support.DataFieldMaxValueIncrementerFactory;
import org.springframework.batch.item.database.support.DefaultDataFieldMaxValueIncrementerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.support.incrementer.DataFieldMaxValueIncrementer;
import org.springframework.transaction.TransactionDefinition;

/**
 * @author Anton Belousov
 * @since 0.0.1-SNAPSHOT
 */
@Configuration
@RequiredArgsConstructor
public class ServiceConfig {

  private final DataSource dataSource;

  @Value("${table.prefix}")
  private String tablePrefix;

  @Bean
  public JdbcTemplate jdbcTemplate() {
    return new JdbcTemplate(dataSource);
  }

  @Bean
  public DataFieldMaxValueIncrementerFactory incrementerFactory() {
    return new DefaultDataFieldMaxValueIncrementerFactory(dataSource);
  }

  @Bean
  public String databaseType() throws SQLException {
    return dataSource.getConnection().getMetaData().getDatabaseProductName();
  }

  @Bean
  public DataFieldMaxValueIncrementer jobInstanceIncrementer() throws SQLException {
    return incrementerFactory().getIncrementer(databaseType(), tablePrefix + "JOB_SEQ");
  }

  @Bean
  public DataFieldMaxValueIncrementer jobExecutionIncrementer() throws SQLException {
    return incrementerFactory().getIncrementer(databaseType(), tablePrefix + "JOB_EXECUTION_SEQ");
  }

  @Bean
  public DataFieldMaxValueIncrementer stepExecutionIncrementer() throws SQLException {
    return incrementerFactory().getIncrementer(databaseType(), tablePrefix + "STEP_EXECUTION_SEQ");
  }

  @Bean
  public Module jacksonConfig() {
    SimpleModule module = new SimpleModule();
    module.addDeserializer(TransactionDefinition.class, new TransactionDefinitionDeserializer());
    return module;
  }
}
