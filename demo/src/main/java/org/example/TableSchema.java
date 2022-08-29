package org.example;
import org.apache.beam.sdk.schemas.JavaFieldSchema;
import org.apache.beam.sdk.schemas.annotations.DefaultSchema;

@DefaultSchema(JavaFieldSchema.class)
public class TableSchema {
    private int id;
    private String name;
    private String surname;

    public TableSchema(int id, String name, String surname){
        this.id = id;
        this.name = name;
        this.surname = surname;
    }
}