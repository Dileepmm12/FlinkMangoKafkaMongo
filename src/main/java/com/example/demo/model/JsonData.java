package com.example.demo.model;

import lombok.Data;
import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.mapping.Document;
import org.springframework.data.mongodb.core.mapping.Field;

@Document(collection = "json_data")
@Data
public class JsonData {

    @Id
    private String id;

    private String name;
    private int age;
    private String tlogId;

}
