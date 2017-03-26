package com.peach;

import com.peach.utils.SchemaUtil;
import org.apache.avro.Schema;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;

/**
 *
 * Created by peach on 2017/3/23.
 * 动态生成avro数据
 */
public class GenerateData {

    private static Logger logger = LoggerFactory.getLogger(GenerateData.class);

    private static String avscpath;
    private static String sourcePath = "/Users/zoulihan/Desktop/customer_address.dat";

    static {
        avscpath = GenerateData.class.getClassLoader().getResource("CustomerAddress.avsc").getPath();
    }


    public static void main(String[] args) {
        if(avscpath == null) {
            logger.error("[peach], avsc file path can not be null");
            System.exit(0);
        }

        try {
            Schema schema = new Schema.Parser().parse(new File(avscpath));
            if(schema == null) {
                logger.error("[peach], schema is null");
                System.exit(0);
            }
            long start = System.currentTimeMillis();

            File datafile = new File("/Users/zoulihan/Desktop/customeraddress2.avro");
            DatumWriter<GenericRecord> datumWriter = new GenericDatumWriter<>(schema);
            DataFileWriter<GenericRecord> dataFileWriter = new DataFileWriter<>(datumWriter);
            dataFileWriter.create(schema, datafile);
            loadData(dataFileWriter, schema);

            dataFileWriter.close();

            logger.info("[peach], compiling the schema elapsed time:{}", System.currentTimeMillis() - start);

        } catch(Exception e) {
            e.printStackTrace();
        }

    }

    /**
     * 加载数据
     * @param dataFileWriter
     * @param schema
     */
    private static void loadData(DataFileWriter<GenericRecord> dataFileWriter, Schema schema) {
        File file = new File(sourcePath);
        if(file == null) {
            logger.error("[peach], source data not found");
            return ;
        }

        InputStreamReader inputStreamReader = null;
        BufferedReader bufferedReader = null;
        try {
            inputStreamReader = new InputStreamReader(new FileInputStream(file));
            bufferedReader = new BufferedReader(inputStreamReader);
            String line;
            GenericRecord genericRecord;
            while((line = bufferedReader.readLine()) != null) {
                if(line != "") {
                    String[] values = line.split("\\|");
                    genericRecord = SchemaUtil.convertRecord(values, schema);
                    if(genericRecord != null) {
                        dataFileWriter.append(genericRecord);
                    }
                }
            }

        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {
                if(bufferedReader != null) {
                    bufferedReader.close();
                }
                if(inputStreamReader != null) {
                    inputStreamReader.close();
                }
            } catch (IOException e) {
            }
        }

    }

    /**
     * 生成记录
     * @param line 读取的行数据
     * @param schema
     * @return GenericRecord记录
     */
    /*private static GenericRecord getCustomerAddress(String line, Schema schema) {
        if(line == null || line == "") {
            return null;
        }
        GenericRecord genericRecord = null;

        String[] values = line.split("\\|");
        if(values != null && values.length == 13) {
            try {
                genericRecord = new GenericData.Record(schema);
                List<Schema.Field> fields = schema.getFields();
                for(Schema.Field field : fields) {
                    Schema.Type type = field.schema().getType();
                    String key = field.name();
                    int position = field.pos();
                    if(type.equals(Schema.Type.LONG)) {
                        genericRecord.put(key, Long.parseLong(values[position]));
                    } else if(type.equals(Schema.Type.DOUBLE)) {
                        String doubleValue = "0";
                        if (values[position] != null && values[position] == "") {
                            doubleValue = values[position];
                        }
                        genericRecord.put(key, Double.parseDouble(doubleValue));
                    } else if(type.equals(Schema.Type.STRING)) {
                        genericRecord.put(key, values[position]);
                    } else {
                        genericRecord.put(key, values[position]);
                    }
                }
            } catch (Exception e) {
                e.printStackTrace();
            } finally {
            }

        }

        return genericRecord;
    }*/


}
