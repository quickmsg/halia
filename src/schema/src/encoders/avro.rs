// use apache_avro::types::Record;
// use apache_avro::Writer;
// // a writer needs a schema and something to write to
// let mut writer = Writer::new(&schema, Vec::new());

// // the Record type models our Record schema
// let mut record = Record::new(writer.schema()).unwrap();
// record.put("a", 27i64);
// record.put("b", "foo");

// // schema validation happens here
// writer.append(record).unwrap();

// // this is how to get back the resulting avro bytecode
// // this performs a flush operation to make sure data has been written, so it can fail
// // you can also call `writer.flush()` yourself without consuming the writer
// let encoded = writer.into_inner().unwrap();