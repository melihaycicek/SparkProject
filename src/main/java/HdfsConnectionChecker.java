import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class HdfsConnectionChecker {
    private static final int MAX_RETRY_ATTEMPTS = 10; // Bağlantı deneme sayısı
    private static final long RETRY_INTERVAL_MS = 3000; // Her deneme arasındaki süre (3 saniye)

    public static void main(String[] args) {
        Configuration conf = new Configuration();
        // HDFS yapılandırma ayarlarını ekleyin
        conf.set("fs.defaultFS", "hdfs://172.20.0.2:8020");
        conf.set("dfs.client.use.datanode.hostname", "true");

        String hdfsPath = "hdfs://172.20.0.2:8020"; // HDFS yolunu buraya ekleyin

        int retryCount = 0;
        boolean isConnected = false;

        while (!isConnected && retryCount < MAX_RETRY_ATTEMPTS) {
            try {
                FileSystem hdfs = FileSystem.get(conf);
                if (hdfs.exists(new Path("/"))) { // HDFS'in kök dizinine erişim denemesi
                    isConnected = true;
                    System.out.println("Bağlantı başarılı!");
                } else {
                    System.out.println("HDFS yolunu bulamadı, tekrar deneyecek...");
                }
            } catch (Exception e) {
                System.out.println("Bağlantı hatası: " + e.getMessage());
            }

            retryCount++;
            if (!isConnected && retryCount < MAX_RETRY_ATTEMPTS) {
                try {
                    Thread.sleep(RETRY_INTERVAL_MS);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }

        if (!isConnected) {
            System.out.println("HDFS'ye bağlanılamadı.");
        }
    }
}
