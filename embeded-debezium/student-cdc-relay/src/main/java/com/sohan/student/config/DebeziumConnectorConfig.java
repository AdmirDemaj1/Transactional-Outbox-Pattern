package com.sohan.student.config;

import java.util.Properties;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * This class provides the configurations required to setup a Debezium connector for the Student Table.
 *
 * @author Sohan
 */
@Configuration
public class DebeziumConnectorConfig {

    @Value("${student.datasource.host}")
    private String studentDBHost;

    @Value("${student.datasource.databasename}")
    private String studentDBName;

    @Value("${student.datasource.port}")
    private String studentDBPort;

    @Value("${student.datasource.username}")
    private String studentDBUserName;

    @Value("${student.datasource.password}")
    private String studentDBPassword;

    private final String STUDENT_TABLE_NAME = "public.student";

    @Bean
    public io.debezium.config.Configuration studentConnector() {
        return io.debezium.config.Configuration.create()
            .with("name", "student-postgres-connector")
            .with("connector.class", "io.debezium.connector.postgresql.PostgresConnector")
            .with("database.hostname", studentDBHost)
            .with("database.port", studentDBPort)
            .with("database.user", studentDBUserName)
            .with("database.password", studentDBPassword)
            .with("database.dbname", studentDBName)
            .with("topic.prefix", "student-postgres")  // Added this line
            .with("table.include.list", STUDENT_TABLE_NAME)
            .with("plugin.name", "pgoutput")
            .with("slot.name", "my_slot_name02")
            
            // Connection stability settings
            .with("max.queue.size", "8192")
            .with("max.batch.size", "2048")
            .with("poll.interval.ms", "1000")
            .with("heartbeat.interval.ms", "5000")
            .with("database.tcpKeepAlive", "true")
            
            // Snapshot settings
            .with("snapshot.mode", "initial")
            .with("snapshot.fetch.size", "10240")
            .with("snapshot.lock.timeout.ms", "10000")
            
            // Storage settings
            .with("offset.storage", "org.apache.kafka.connect.storage.FileOffsetBackingStore")
            .with("offset.storage.file.filename", "/tmp/offsets.dat")
            .with("offset.flush.interval.ms", "60000")
            .with("schema.history.internal", "io.debezium.storage.file.history.FileSchemaHistory")
            .with("schema.history.internal.file.filename", "/tmp/schema-history.dat")
            
            .build();
    }
}