package com.peach.utils;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * Schema工具类
 *
 * Created by peach on 2017/3/24.
 */
public class SchemaUtil {

    private static final Logger logger = LoggerFactory.getLogger(SchemaUtil.class);

    /**
     * 将数据转换为GenericRecord记录
     * @param values 数组
     * @param schema
     * @return
     */
    public static GenericRecord convertRecord(String[] values, Schema schema) {

        if(schema == null || values == null) {
            return null;
        }

        List<Schema.Field> fields = schema.getFields();
        if(fields == null || fields.size() == 0) {
            return null;
        }

        if(values.length < fields.size()) { //过滤无效数据
            return null;
        }
        GenericRecord genericRecord = new GenericData.Record(schema);
        for(Schema.Field field : fields) {
            String name = field.name();
            Schema.Type type = field.schema().getType();
            Object value = typeConvert(type, values[field.pos()]);
            genericRecord.put(name, value);
        }
        return genericRecord;
    }


    /**
     * 根据schema字段类型将值转换为对应数据类型
     * @param type
     * @param value
     * @return
     */
    public static Object typeConvert(Schema.Type type, String value) {

        if(value != null)
            value = value.trim();

        Object object = null;
        try {
            if(type.equals(Schema.Type.NULL)) {
                object = null;
            }else if(type.equals(Schema.Type.LONG)) {
                if(value == null || "".equals(value)) {
                    value = "0";
                }
                object = Long.parseLong(value);
            } else if(type.equals(Schema.Type.STRING)) {
                object = value;
            } else if(type.equals(Schema.Type.INT)) {
                if(value == null || "".equals(value)) {
                    value = "0";
                }
                object = Integer.parseInt(value);
            } else if(type.equals(Schema.Type.FLOAT)) {
                if(value == null || "".equals(value)) {
                    value = "0";
                }
                object = Float.parseFloat(value);
            } else if(type.equals(Schema.Type.DOUBLE)) {
                if(value == null || "".equals(value)) {
                    value = "0";
                }
                object = Double.parseDouble(value);
            } else if(type.equals(Schema.Type.BYTES)) {
                if(value != null || !"".equals(value)) {
                    object = value.getBytes();
                }
            } else if(type.equals(Schema.Type.BOOLEAN)) {
                if(value != null || !"".equals(value)) {
                    object = Boolean.parseBoolean(value);
                }
            } else {
                object = value;
            }
        } catch (NumberFormatException e) {
            logger.error("[peach], type convert failed...value:{}, type:{}", value, type);
        }
        return object;
    }

}
