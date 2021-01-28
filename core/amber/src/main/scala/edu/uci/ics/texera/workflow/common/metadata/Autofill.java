package edu.uci.ics.texera.workflow.common.metadata;
import java.lang.annotation.*;
import com.fasterxml.jackson.annotation.JacksonAnnotationsInside;
import com.kjetland.jackson.jsonSchema.annotations.JsonSchemaInject;

@Retention(RetentionPolicy.RUNTIME)
@Target({ElementType.FIELD})
@JacksonAnnotationsInside
@JsonSchemaInject(json = "{ \"autofill\": \"true\" } ")
public @interface Autofill {}