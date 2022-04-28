package com.mininglamp.nebulamybatis;

import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import javax.sql.DataSource;

/**
 * @author zzh
 */
@Configuration
@ConditionalOnClass(DataSource.class)
public class NebulaMybatisAutoConfiguration {

    @Bean
    public GraphResultSetIntercept graphResultSetIntercept() {
        return new GraphResultSetIntercept();
    }

}
