package com.sohan.student.listener;

import com.sohan.student.elasticsearch.service.StudentService;
import com.sohan.student.utils.Operation;
import io.debezium.config.Configuration;
import io.debezium.embedded.EmbeddedEngine;
import io.debezium.engine.ChangeEvent;
import io.debezium.engine.DebeziumEngine;
import io.debezium.engine.format.Json;
import lombok.extern.slf4j.Slf4j;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import java.util.Map;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;



import static io.debezium.data.Envelope.FieldName.*;
import static java.util.stream.Collectors.toMap;

import java.io.IOException;

/**
 * This class creates, starts and stops the EmbeddedEngine, which starts the Debezium engine. The engine also
 * loads and launches the connectors setup in the configuration.
 * <p>
 * The class uses @PostConstruct and @PreDestroy functions to perform needed operations.
 *
 * @author Sohan
 */
@Component
public class CDCListener {

    /**
     * Single thread pool which will run the Debezium engine asynchronously.
     */
    private final Executor executor = Executors.newSingleThreadExecutor();



    /**
     * The Debezium engine which needs to be loaded with the configurations, Started and Stopped - for the
     * CDC to work.
     */
    private final DebeziumEngine<ChangeEvent<String, String>> engine;

    /**
     * Handle to the Service layer, which interacts with ElasticSearch.
     */
    private final StudentService studentService;

    
    

    /**
     * Constructor which loads the configurations and sets a callback method 'handleEvent', which is invoked when
     * a DataBase transactional operation is performed.
     *
     * @param studentConnector
     * @param studentService
     * @throws IOException 
     */
    private CDCListener(Configuration studentConnector, StudentService studentService) throws IOException {
        // Create the engine with this configuration ...
        try (DebeziumEngine<ChangeEvent<String, String>> engine = DebeziumEngine.create(Json.class)
                .using(studentConnector.asProperties())
                .notifying(record -> {
                    log.info("bla bla blaaaa");
                    System.out.println(record);
                }).build()
            ) {

                this.engine = engine;

                // Run the engine asynchronously ...
                ExecutorService executor = Executors.newSingleThreadExecutor();
                executor.execute(engine);

                // Do something else or wait for a signal or an event
              
                // Engine is stopped when the main code is finished
        }

        this.studentService = studentService;
    }

    private static final Logger log = LoggerFactory.getLogger(CDCListener.class);

    /**
     * The method is called after the Debezium engine is initialized and started asynchronously using the Executor.
     */
    @PostConstruct
    private void start() {
        this.executor.execute(engine);
    }

    /**
     * This method is called when the container is being destroyed. This stops the debezium, merging the Executor.
     */
    @PreDestroy
    private void stop() {
        if (this.engine != null) {
            try {
                this.engine.close();
            } catch (IOException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
        }
    }

    /**
     * This method is invoked when a transactional action is performed on any of the tables that were configured.
     *
     * @param sourceRecord
     */
   private void handleEvent(SourceRecord sourceRecord) {
    Struct sourceRecordValue = (Struct) sourceRecord.value();

    if (sourceRecordValue != null) {
        log.info("Received record: {}", sourceRecordValue);
        Operation operation = Operation.forCode((String) sourceRecordValue.get(OPERATION));
        
        if (operation != Operation.READ) {
            log.info("Operation: {}", operation);

            Map<String, Object> message;
            String record = AFTER; // For Update & Insert operations.

            if (operation == Operation.DELETE) {
                record = BEFORE; // For Delete operations.
            }

            Struct struct = (Struct) sourceRecordValue.get(record);
            message = struct.schema().fields().stream()
                    .map(Field::name)
                    .filter(fieldName -> struct.get(fieldName) != null)
                    .map(fieldName -> Pair.of(fieldName, struct.get(fieldName)))
                    .collect(toMap(Pair::getKey, Pair::getValue));

            log.info("Message data: {}", message);
            this.studentService.maintainReadModel(message, operation);
            log.info("Data Changed: {} with Operation: {}", message, operation.name());
        }
    }
}

}