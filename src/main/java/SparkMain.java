

import com.sun.javafx.runtime.SystemProperties;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQueryException;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Properties;
import java.util.logging.Logger;

public class SparkMain {
    private static final Logger logger = Logger.getLogger(SparkMain.class.getName());
    public static void main(String[] args) throws StreamingQueryException {

        // String mainDirCall = args.length > 0 ? args[0] : "/";

        String propertiesPath = "SparkProject.properties"; // Düzeltildi
        //
        Properties properties = new Properties();
        try (InputStream input = SparkMain.class.getResourceAsStream("/SparkProject.properties")) {
            if (input == null) {
                System.out.println("Sorry, unable to find SparkProject.properties");
                return;
            }
            // Dosyadan özellikleri yükle
            properties.load(input);
        } catch (IOException ex) {
            ex.printStackTrace();
        }


        /*
        Properties properties = new Properties();
        try (InputStream input = new FileInputStream(propertiesPath)) {
            properties.load(input);
            // Properties dosyasını kullan
        } catch (IOException ex) {
            ex.printStackTrace();
        }*/
        String mainDirCall = properties.getProperty("main.dir");
        //String redisHost = properties.getProperty("redis.host");
        //int redisPort = Integer.parseInt(properties.getProperty("redis.port"));
        //int redisTimeout = Integer.parseInt(properties.getProperty("redis.timeout"));
        //String redisPassword = properties.getProperty("redis.password");
        String hdfsPath = properties.getProperty("hdfs.path");
        String mainDirSms = properties.getProperty("main.dir.sms");
        System.out.println("Main Directory: " + mainDirCall);

        Path path = Paths.get(mainDirCall);


        if (!Files.exists(path)) {
            try {
                // Dizin yoksa oluştur
                Files.createDirectories(path);
                logger.info("Directory created: " + mainDirCall);
            } catch (IOException e) {
                e.printStackTrace();
                logger.info("Failed to create directory: " + mainDirCall);
            }
        } else {
            System.out.println("Directory already exists: " + mainDirCall);
        }

// Özellik dosyasında spark.master=local[*] gibi bir değer olmalı
        String sparkMasterUrl = properties.getProperty("spark.master", "local[*]"); // varsayılan değer olarak local[*] kullanılıyor
        SparkConf conf = new SparkConf().setMaster(sparkMasterUrl)
                .setAppName("Spark Kafka Reader")
                .set("spark.sql.crossJoin.enabled", "true")
                .set("spark.executor.instances", "2");



        SparkSession spark = SparkSession.builder()
                .appName("Structured Streaming File Processing")
                .config(conf)
                //.config("spark.executor.extraJavaOptions", "-Dconfig.file.path=" + configFile)
                .getOrCreate();

       // System.setProperty("java.security.auth.login.config", System.getProperty("krb.login.config"));
        //System.setProperty("sun.security.jgss.debug","true");
        //System.setProperty("javax.security.auth.useSubjectCredsOnly","false");
        //System.setProperty("java.security.krb5.conf", SystemProperties.getProperty("krb.krb5.config"));

        try {
            ProcessTypeTwo.process(spark, mainDirCall,hdfsPath);

        } catch (Exception e) {
            e.printStackTrace();
        }

        spark.streams().awaitAnyTermination();
    }
}
