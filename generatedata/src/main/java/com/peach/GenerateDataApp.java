package com.peach;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.util.StringTokenizer;

import org.apache.avro.file.DataFileWriter;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.specific.SpecificDatumWriter;

import com.peach.arvo.CustomerAddress;

/**
 * @author  peach
 * 2017-03-02
 * 主要用于生成avro数据文件
 */
public class GenerateDataApp {
//    private static String customerAddress_avsc_path;
//
//    static {
//        customerAddress_avsc_path = GenerateDataApp.class.getClass().getResource("/CustomerAddress.avsc").getPath();
//    }
    private static String source_data_path = "F:\\data\\customer_address.dat"; //源数据文件路径
    private static String dest_avro_data_path = "F:\\data\\customeraddress.avro"; //生成的avro数据文件路径

    public static void main(String[] args) {

        try {
//            if(customerAddress_avsc_path != null) {
//                File file = new File(customerAddress_avsc_path);
//                Schema schema = new Schema.Parser().parse(file);
//            }
            DatumWriter<CustomerAddress> caDatumwriter = new SpecificDatumWriter<>(CustomerAddress.class);
            DataFileWriter<CustomerAddress> dataFileWriter = new DataFileWriter<>(caDatumwriter);
            dataFileWriter.create(new CustomerAddress().getSchema(), new File(dest_avro_data_path));
            loadData(dataFileWriter);
            dataFileWriter.close();
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    /**
     * 加载源数据文件
     * @param dataFileWriter
     */
    private static void loadData(DataFileWriter<CustomerAddress> dataFileWriter) {

        File file = new File(source_data_path);
        if(!file.isFile()) {
            return;
        }
        try {
            InputStreamReader isr = new InputStreamReader(new FileInputStream(file));
            BufferedReader reader = new BufferedReader(isr);
            String line;
            CustomerAddress address;
            while ((line = reader.readLine()) != null) {
                address = getCustomerAddress(line);
                if (address != null) {
                    dataFileWriter.append(address);
                }
            }
            isr.close();
            reader.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * 通过记录封装CustomerAddress对象
     * @param line
     * @return
     */
    private static CustomerAddress getCustomerAddress(String line) {
        CustomerAddress ca = null;
        try {
            if (line != null && line != "") {
                StringTokenizer token = new StringTokenizer(line, "|"); //使用stringtokenizer拆分字符串时，会去自动除""类型
                if(token.countTokens() >= 13) {
                    ca = new CustomerAddress();
                    ca.setCaAddressSk(Long.parseLong(token.nextToken()));
                    ca.setCaAddressId(token.nextToken());
                    ca.setCaStreetNumber(token.nextToken());
                    ca.setCaStreetName(token.nextToken());
                    ca.setCaStreetType(token.nextToken());
                    ca.setCaSuiteNumber(token.nextToken());
                    ca.setCaCity(token.nextToken());
                    ca.setCaCounty(token.nextToken());
                    ca.setCaState(token.nextToken());
                    ca.setCaZip(token.nextToken());
                    ca.setCaCountry(token.nextToken());
                    ca.setCaGmtOffset(Double.parseDouble(token.nextToken()));
                    ca.setCaLocationType(token.nextToken());
                } else {
                    System.err.println(line);
                }
            }
        } catch (NumberFormatException e) {
            System.err.println(line);
        }

        return ca;
    }
}
