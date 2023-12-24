// SparkService.java

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.SparkConf;

public class SparkService {

    private static SparkConf loadSparkConfiguration() {
        // Properties dosyasından okuma veya manuel ayarlamalar yapabilirsiniz
        String appName = "MySparkApplication";
        String masterUrl = "local[*]";
        String executorMemory = "2g";
        String driverMemory = "1g";

        SparkConf conf = new SparkConf()
                .setAppName(appName)
                .setMaster(masterUrl)
                .set("spark.executor.memory", executorMemory)
                .set("spark.driver.memory", driverMemory);

        // Diğer konfigürasyonları ekleyebilirsiniz

        return conf;
    }

    public static void main(String[] args) {

        SparkConf conf = loadSparkConfiguration();

        // JavaSparkContext oluştur
        JavaSparkContext sc = new JavaSparkContext(conf);


        // Input ve output dizinleri
        String sourcePath = "hdfs://<your-hdfs-master>/Source";  // HDFS veya local path
        String targetPath = "hdfs://<your-hdfs-master>/Target";  // HDFS veya local path

        // Dosyaları oku
        JavaRDD<String> inputRDD = sc.textFile(sourcePath);

        // İşlemleri gerçekleştir (örneğin, her satırı büyük harfe çevir)
        JavaRDD<String> outputRDD = inputRDD.map(String::toUpperCase);

        // Sonuçları hedef dizine yaz
        outputRDD.saveAsTextFile(targetPath);

        // Spark bağlantısını kapat
        sc.close();
    }
}
