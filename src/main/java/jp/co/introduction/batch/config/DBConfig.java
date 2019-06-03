package jp.co.introduction.batch.config;

import java.util.Properties;

import javax.sql.DataSource;

import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.jdbc.datasource.DriverManagerDataSource;
import org.springframework.orm.jpa.LocalContainerEntityManagerFactoryBean;
import org.springframework.orm.jpa.vendor.HibernateJpaVendorAdapter;

/**
 * DB接続Configクラス
 */
@Configuration
public class DBConfig {

  @Bean
  public DataSource dataSource() {
    DriverManagerDataSource dataSource = new DriverManagerDataSource();
    dataSource.setDriverClassName("com.mysql.cj.jdbc.Driver");
    dataSource.setUrl(
        "jdbc:mysql://127.0.0.1:3306/mysql?characterEncoding=UTF-8&serverTimezone=JST&useSSL=false&allowPublicKeyRetrieval=true");
    dataSource.setUsername("root"); // ローカルで起動しているRDBのユーザ名を設定
    dataSource.setPassword("Password"); // ローカルで起動しているRDBのパスワードを設定
    return dataSource;
  }

  @Bean
  Properties additionalProperties() {
    Properties hibernateProperties = new Properties();
    hibernateProperties.setProperty("hibernate.hbm2ddl.auto", "create-drop");
    hibernateProperties.setProperty("hibernate.dialect", "org.hibernate.dialect.MySQLDialect");
    hibernateProperties.setProperty("hibernate.show_sql", "true");
    return hibernateProperties;
  }

  @Bean
  @ConditionalOnBean(name = { "dataSource", "additionalProperties" })
  @ConditionalOnMissingBean
  public LocalContainerEntityManagerFactoryBean entityManagerFactory() {
    LocalContainerEntityManagerFactoryBean em = new LocalContainerEntityManagerFactoryBean();
    em.setDataSource(dataSource());
    em.setPackagesToScan("jp.co.introduction.batch");
    em.setJpaVendorAdapter(new HibernateJpaVendorAdapter());
    em.setJpaProperties(additionalProperties());
    return em;
  }
}
