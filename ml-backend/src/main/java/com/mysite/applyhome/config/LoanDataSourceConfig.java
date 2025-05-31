package com.mysite.applyhome.config;

import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.jdbc.DataSourceBuilder;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.jpa.repository.config.EnableJpaRepositories;
import org.springframework.orm.jpa.JpaTransactionManager;
import org.springframework.orm.jpa.LocalContainerEntityManagerFactoryBean;
import org.springframework.orm.jpa.vendor.HibernateJpaVendorAdapter;
import org.springframework.transaction.PlatformTransactionManager;

import javax.sql.DataSource;
import java.util.HashMap;

@Configuration
@EnableJpaRepositories(
    basePackages = "com.mysite.applyhome.loan",
    entityManagerFactoryRef = "loanEntityManagerFactory",
    transactionManagerRef = "loanTransactionManager"
)
public class LoanDataSourceConfig {

    @Bean(name = "loanDataSource")
    @ConfigurationProperties(prefix = "spring.datasource.loan")
    public DataSource loanDataSource() {
        return DataSourceBuilder.create().build();
    }

    @Bean(name = "loanEntityManagerFactory")
    public LocalContainerEntityManagerFactoryBean loanEntityManagerFactory(
            @Qualifier("loanDataSource") DataSource dataSource) {
        LocalContainerEntityManagerFactoryBean em = new LocalContainerEntityManagerFactoryBean();
        em.setDataSource(dataSource);
        em.setPackagesToScan("com.mysite.applyhome.loan");

        HibernateJpaVendorAdapter vendorAdapter = new HibernateJpaVendorAdapter();
        em.setJpaVendorAdapter(vendorAdapter);

        HashMap<String, Object> properties = new HashMap<>();
        properties.put("hibernate.hbm2ddl.auto", "none");
        properties.put("hibernate.dialect", "org.hibernate.dialect.MySQLDialect");
        em.setJpaPropertyMap(properties);

        return em;
    }

    @Bean(name = "loanTransactionManager")
    public PlatformTransactionManager loanTransactionManager(
            @Qualifier("loanEntityManagerFactory") LocalContainerEntityManagerFactoryBean loanEntityManagerFactory) {
        return new JpaTransactionManager(loanEntityManagerFactory.getObject());
    }
} 