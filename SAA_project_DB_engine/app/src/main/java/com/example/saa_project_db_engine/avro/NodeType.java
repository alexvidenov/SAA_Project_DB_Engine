package com.example.saa_project_db_engine.avro;

/**
 * Autogenerated by Avro
 *
 * DO NOT EDIT DIRECTLY
 */
@org.apache.avro.specific.AvroGenerated
public enum NodeType implements org.apache.avro.generic.GenericEnumSymbol<NodeType> {
  RootNode, InternalNode, LeafNode  ;
  public static final org.apache.avro.Schema SCHEMA$ = new org.apache.avro.Schema.Parser().parse("{\"type\":\"enum\",\"name\":\"NodeType\",\"symbols\":[\"RootNode\",\"InternalNode\",\"LeafNode\"]}");
  public static org.apache.avro.Schema getClassSchema() { return SCHEMA$; }
  public org.apache.avro.Schema getSchema() { return SCHEMA$; }
}
