package com.example.demo.config;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.configuration.CoreOptions;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class FlinkConfig {

    @Bean
    public StreamExecutionEnvironment streamExecutionEnvironment() {
        org.apache.flink.configuration.Configuration flinkConfig = new org.apache.flink.configuration.Configuration();
        flinkConfig.set(CoreOptions.DEFAULT_PARALLELISM, 4);

        StreamExecutionEnvironment env = StreamExecutionEnvironment
                .createLocalEnvironmentWithWebUI(flinkConfig);

        env.setRuntimeMode(RuntimeExecutionMode.BATCH);
        env.setParallelism(4);

        return env;
    }
}
