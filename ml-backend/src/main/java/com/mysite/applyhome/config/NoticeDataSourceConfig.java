package com.mysite.applyhome.config;

import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.jdbc.DataSourceBuilder;
import org.springframework.boot.orm.jpa.EntityManagerFactoryBuilder;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.jpa.repository.config.EnableJpaRepositories;
import org.springframework.orm.jpa.JpaTransactionManager;
import org.springframework.orm.jpa.LocalContainerEntityManagerFactoryBean;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.annotation.EnableTransactionManagement;

import javax.sql.DataSource;
import java.util.HashMap;
import java.util.Map;

@Configuration
@EnableTransactionManagement
@EnableJpaRepositories(
        entityManagerFactoryRef = "noticeEntityManagerFactory",
        transactionManagerRef = "noticeTransactionManager",
        basePackages = {"com.mysite.applyhome.notice"}
)
public class NoticeDataSourceConfig {

    @Bean(name = "noticeDataSource")
    @ConfigurationProperties(prefix = "spring.datasource.notice")
    public DataSource dataSource() {
        return DataSourceBuilder.create().build();
    }

    @Bean(name = "noticeEntityManagerFactory")
    public LocalContainerEntityManagerFactoryBean entityManagerFactory(
            EntityManagerFactoryBuilder builder,
            @Qualifier("noticeDataSource") DataSource dataSource) {
        
        Map<String, Object> properties = new HashMap<>();
        properties.put("hibernate.hbm2ddl.auto", "update");
        properties.put("hibernate.dialect", "org.hibernate.dialect.MySQLDialect");
        
        return builder
                .dataSource(dataSource)
                .packages("com.mysite.applyhome.notice")
                .persistenceUnit("notice")
                .properties(properties)
                .build();
    }

    @Bean(name = "noticeTransactionManager")
    public PlatformTransactionManager transactionManager(
            @Qualifier("noticeEntityManagerFactory") LocalContainerEntityManagerFactoryBean entityManagerFactory) {
        return new JpaTransactionManager(entityManagerFactory.getObject());
    }
} 