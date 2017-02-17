package com.progrexor.data

import java.io.File
import java.math.BigDecimal

import org.apache.avro.file.DataFileWriter
import org.apache.avro.generic.GenericData
import org.apache.avro.generic.GenericData.Record
import org.apache.avro.io.DatumWriter
import org.apache.avro.{Conversions, Schema}

/**
  * Created by andreyd on 14/02/2017.
  */
object AvroDataGen {

  /**
    * Create change record
    *
    * @param schema
    * @return
    */
  def newCtRec(value: Any)(implicit schema: Schema) = {
    new Record(schema)
  }

  /**
    * Main entry
    *
    * @param args
    */
  def main(args: Array[String]): Unit = {

    /** ********************************/
    /* SET PATHS                      */
    /** ********************************/
    val baseSchemaPath = "src/test/resources/user.avsc"
    val baseAvroPath = "src/test/resources/user.avro"

    /** ********************************/
    /* INITIALIZATION                 */
    /** ********************************/
    implicit val schema = getSchema(baseSchemaPath)
    val avro = getAvro(baseAvroPath)

    /** ********************************/
    /* DATA PREPARATION BLOCK         */
    /** ********************************/
    // Decimal
    val dec1 = new BigDecimal("3.123456789012345678")
    val dec2 = dec1.multiply(dec1)
    val dec3 = dec2.multiply(new BigDecimal("-1"))

    val str = (40 to 120).map(_.toChar).foldLeft("")(_ + _)

    /** ********************************/
    /* ADD RECORDS                    */
    /** ********************************/
    avro.append(newRec(str, dec3, dec3))

    /** ********************************/
    /* CLOSE AVRO FILE                */
    /** ********************************/
    avro.close()
  }

  /**
    * Get schema from a schema file
    *
    * @param schemaFile
    * @return
    */
  def getSchema(schemaFile: String) = new Schema.Parser().parse(new File(schemaFile))

  /**
    * Get new Avro file, ready to be written into
    *
    * @param avroFile
    * @param schema
    * @return
    */
  def getAvro(avroFile: String)(implicit schema: Schema) = {
    val genericData = new GenericData()
    genericData.addLogicalTypeConversion(new Conversions.DecimalConversion)
    val datumWriter = genericData.createDatumWriter(schema)
    val dataFileWriter = new DataFileWriter[GenericData.Record](datumWriter.asInstanceOf[DatumWriter[GenericData.Record]])
    dataFileWriter.create(schema, new File(avroFile))
  }

  /**
    * Create base record
    *
    * @param schema
    * @return
    */
  def newRec(str: String, dec1: BigDecimal, dec2: BigDecimal)(implicit schema: Schema) = {
    val rec = new Record(schema)
    rec.put("val_str", str)
    rec.put("default_decimal", dec1)
    rec.put("default_null", dec2)
    rec
  }
}

//TODO: Null values checks
//TODO: UTF-8, UTF-16 and different encodings $@^@&@&&£%****&$
//TODO: Test decimals
//TODO: max(string), max(int)
//TODO: complex types