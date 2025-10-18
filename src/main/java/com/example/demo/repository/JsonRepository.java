package com.example.demo.repository;

import com.example.demo.model.JsonData;
import org.springframework.data.mongodb.repository.MongoRepository;

public interface JsonRepository extends MongoRepository<JsonData, String> {}
