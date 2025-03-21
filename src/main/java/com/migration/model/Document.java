package com.migration.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.databind.JsonNode;

import java.io.Serializable;

/**
 * Generic document model that can represent both Couchbase and DynamoDB documents
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class Document implements Serializable {
    private static final long serialVersionUID = 1L;
    
    private String id;
    private JsonNode content;
    
    public Document() {
    }
    
    public Document(String id, JsonNode content) {
        this.id = id;
        this.content = content;
    }
    
    public String getId() {
        return id;
    }
    
    public void setId(String id) {
        this.id = id;
    }
    
    public JsonNode getContent() {
        return content;
    }
    
    public void setContent(JsonNode content) {
        this.content = content;
    }
    
    @Override
    public String toString() {
        return "Document{" +
                "id='" + id + '\'' +
                ", content=" + content +
                '}';
    }
}
