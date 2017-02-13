package edu.berkeley.cs186.database.table;

import edu.berkeley.cs186.database.databox.*;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * The Schema of a particular table.
 *
 * Properties:
 * `fields`: an ordered list of column names
 * `fieldTypes`: an ordered list of data types corresponding to the columns
 * `size`: physical size (in bytes) of a record conforming to this schema
 */
public class Schema {
  private List<String> fields;
  private List<DataBox> fieldTypes;
  private int size;

  public Schema(List<String> fields, List<DataBox> fieldTypes) {
    assert(fields.size() == fieldTypes.size());

    this.fields = fields;
    this.fieldTypes = fieldTypes;
    this.size = 0;

    for (DataBox dt : fieldTypes) {
      this.size += dt.getSize();
    }
  }

  /**
   * Verifies that a list of DataBoxes corresponds to this schema. A list of
   * DataBoxes corresponds to this schema if the number of DataBoxes in the
   * list equals the number of columns in this schema, and if each DataBox has
   * the same type and size as the columns in this schema.
   *
   * @param values the list of values to check
   * @return a new Record with the DataBoxes specified
   * @throws SchemaException if the values specified don't conform to this Schema
   */
  public Record verify(List<DataBox> values) throws SchemaException {
    // TODO: implement me!
    int num = this.fields.size();
    boolean indicator = (values.size() == num);

    if (!indicator) {
      throw new SchemaException("Size of DataBoxes does not match size of this schema");
    }

    for (int i = 0; i < num; i++) {
      DataBox a = values.get(i);
      DataBox b = this.fieldTypes.get(i);

      if (a.type() != b.type() || a.getSize() != b.getSize()) {
        indicator = false;
        break;
      }
    }

    if (!indicator) {
      throw new SchemaException("List of DataBoxes does not correspond to fields of this schema");
    }

    return new Record(values);
  }

  /**
   * Serializes the provided record into a byte[]. Uses the DataBoxes'
   * serialization methods. A serialized record is represented as the
   * concatenation of each serialized DataBox. This method assumes that the
   * input record corresponds to this schema.
   *
   * @param record the record to encode
   * @return the encoded record as a byte[]
   */
  public byte[] encode(Record record) {
    // TODO: implement me!
    List<DataBox> values = record.getValues();
    byte[] code = new byte[this.size];
    int num = this.fieldTypes.size();
    int byteIndex = 0;

    for (int i = 0; i < num; i++) {

      DataBox box = values.get(i);
      int boxSize = box.getSize();
      byte[] boxBytes = box.getBytes();

      for (int j = 0; j < boxSize; j++) {
        code[byteIndex + j] = boxBytes[j];
      }

      byteIndex += boxSize;
    }
    return code;
  }

  /**
   * Takes a byte[] and decodes it into a Record. This method assumes that the
   * input byte[] represents a record that corresponds to this schema.
   *
   * @param input the byte array to decode
   * @return the decoded Record
   */
  public Record decode(byte[] input) {
    // TODO: implement me!
    int num = this.fieldTypes.size();
    List<DataBox> values = new ArrayList<DataBox>();
    int byteIndex = 0;

    for (int i = 0; i < num; i++) {
      DataBox sampleBox = this.fieldTypes.get(i);
      int boxSize = sampleBox.getSize();
      byte[] boxByte = new byte[boxSize];

      for (int j = 0; j < boxSize; j++) {
        boxByte[j] = input[j + byteIndex];
      }

      if (sampleBox.type().equals(DataBox.Types.BOOL)) {

        BoolDataBox box = new BoolDataBox(boxByte);
        values.add(box);

      } else if (sampleBox.type().equals(DataBox.Types.FLOAT)) {

        FloatDataBox box = new FloatDataBox(boxByte);
        values.add(box);

      } else if (sampleBox.type().equals(DataBox.Types.INT)) {

        IntDataBox box = new IntDataBox(boxByte);
        values.add(box);

      } else {

        StringDataBox box = new StringDataBox(boxByte);
        values.add(box);

      }

      byteIndex += boxSize;
    }
    return new Record(values);
  }

  public int getEntrySize() {
    return this.size;
  }

  public List<String> getFieldNames() {
    return this.fields;
  }

  public List<DataBox> getFieldTypes() {
    return this.fieldTypes;
  }

  @Override
  public boolean equals(Object other) {
    if (!(other instanceof Schema)) {
      return false;
    }

    Schema otherSchema = (Schema) other;

    if (this.fields.size() != otherSchema.fields.size()) {
      return false;
    }

    for (int i = 0; i < this.fields.size(); i++) {
      DataBox thisType = this.fieldTypes.get(i);
      DataBox otherType = otherSchema.fieldTypes.get(i);

      if (thisType.type() != otherType.type()) {
        return false;
      }

      if (thisType.type().equals(DataBox.Types.STRING) && thisType.getSize() != otherType.getSize()) {
        return false;
      }
    }

    return true;
  }
}
