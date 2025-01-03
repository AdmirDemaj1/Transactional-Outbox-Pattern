package com.sohan.student.listener;

import lombok.Builder;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.sohan.student.elasticsearch.service.StudentService;
import com.sohan.student.elasticsearch.entity.Student;
import com.sohan.student.utils.Operation;
import io.debezium.config.Configuration;
import io.debezium.engine.ChangeEvent;
import io.debezium.engine.DebeziumEngine;
import io.debezium.engine.format.Json;
import lombok.extern.slf4j.Slf4j;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import java.util.Set;
import java.util.Arrays;
import java.util.HashSet;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;

import com.google.gson.Gson;

@Component
@Slf4j
public class CDCListener {
    private final Executor executor = Executors.newSingleThreadExecutor();
    private final DebeziumEngine<ChangeEvent<String, String>> engine;
    private final StudentService studentService;
    private final KafkaProducer<String, String> producer;

    private static final Logger log = LoggerFactory.getLogger(CDCListener.class);
    
    @Value("${kafka.topic.student}")
    private String studentTopic;

    @Autowired
    public CDCListener(Configuration studentConnector, 
                      StudentService studentService,
                      KafkaProducer<String, String> kafkaProducer) throws IOException {
        this.studentService = studentService;
        this.producer = kafkaProducer;
        
        this.engine = DebeziumEngine.create(Json.class)
            .using(studentConnector.asProperties())
            .notifying(this::handleEvent)
            .build();
    }

private void handleEvent(ChangeEvent<String, String> event) {
        String value = event.value();
        if (value != null) {
            try {
                log.info("Received event: {}", value);
                JsonObject jsonObject = JsonParser.parseString(value).getAsJsonObject();
                JsonElement payload = jsonObject.get("payload");
                
                if (payload != null && payload.isJsonObject()) {
                    JsonObject payloadObject = payload.getAsJsonObject();
                    JsonElement opElement = payloadObject.get("op");
                    
                    if (opElement != null) {
                        String operation = opElement.getAsString();
                        
                        if ("c".equals(operation)) {
                            JsonElement afterElement = payloadObject.get("after");
                            if (afterElement != null && afterElement.isJsonObject()) {
                                JsonObject afterObject = afterElement.getAsJsonObject();
                                Student student = new Student();
                                
                                if (afterObject.has("id")) {
                                    student.setId(afterObject.get("id").getAsInt());
                                }
                                if (afterObject.has("name")) {
                                    student.setName(afterObject.get("name").getAsString());
                                }
                                if (afterObject.has("address")) {
                                    student.setAddress(afterObject.get("address").getAsString());
                                }
                                if (afterObject.has("email")) {
                                    student.setEmail(afterObject.get("email").getAsString());
                                }

                                ProducerRecord<String, String> record = 
                                    new ProducerRecord<>(studentTopic, new Gson().toJson(student));
                                
                                producer.send(record, (metadata, exception) -> {
                                    if (exception == null) {
                                        log.info("Message sent successfully to partition {}", 
                                            metadata.partition());
                                    } else {
                                        log.error("Failed to send message", exception);
                                    }
                                });
                            }
                        }
                        
                        Map<String, Object> studentMap = extractStudentData(payloadObject);
                        studentService.maintainReadModel(studentMap, Operation.forCode(operation));
                    }
                }
            } catch (Exception e) {
                log.error("Error processing event", e);
            }
        }
    }

private Map<String, Object> extractStudentData(JsonObject payload) {
    Map<String, Object> studentMap = new HashMap<>();
    if (payload.has("after")) {
        JsonObject after = payload.getAsJsonObject("after");
        if (after.has("id")) studentMap.put("id", after.get("id").getAsInt());
        if (after.has("name")) studentMap.put("name", after.get("name").getAsString());
        if (after.has("address")) studentMap.put("address", after.get("address").getAsString());
        if (after.has("email")) studentMap.put("email", after.get("email").getAsString());
    }
    return studentMap;
}

    @PostConstruct
    private void start() {
        this.executor.execute(engine);
    }

    @PreDestroy
    private void stop() {
        if (producer != null) {
            producer.close();
        }
        if (engine != null) {
            try {
                engine.close();
            } catch (IOException e) {
                log.error("Error closing engine", e);
            }
        }
    }
}