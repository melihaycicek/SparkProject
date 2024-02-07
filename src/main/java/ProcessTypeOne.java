
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.ForeachWriter;
//import redis.clients.jedis.Jedis;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;

import java.io.IOException;
import java.util.logging.Logger;


public class ProcessTypeOne {

    /**
     *
     *
     * @param spark The SparkSession object.
     * @param mainDir The directory of the data to be read.
     * @param redisHost The hostname of the Redis server.
     * @param redisPort The port number of the Redis server.
     * @param redisPassword The password for the Redis server.
     * @param redisTimeout The timeout duration for the Redis connection.
     * @param hdfsPath The file system path in HDFS where the processed data will be written.
     *
     */
    private static final Logger logger = Logger.getLogger(ProcessTypeOne.class.getName());
    public static void process(SparkSession spark, String mainDir, String redisHost, int redisPort, String redisPassword, int redisTimeout, String hdfsPath) throws Exception {


        spark = SparkSession.builder().appName("Structured Streaming File Processing")
                .getOrCreate();


        Dataset<Row> lines = spark.readStream()
                .format("text")
                .load(mainDir);
        logger.info("Starting to read data stream");
        // Satir islem
        lines.writeStream().foreach(new ForeachWriter<Row>() {
           // private Jedis redis;
            private FileSystem hdfs;
            private StringBuilder accumulatedLines;

            @Override
            public boolean open(long partitionId, long version) {
                logger.info("Opening connection for partition: " + partitionId);
                // Redis bağlantısını aç ve HDFS konfigürasyonunu yap.
                //redis = new Jedis(redisHost, redisPort, redisTimeout);
                //redis.auth(redisPassword);
                accumulatedLines = new StringBuilder();
                Configuration conf = new Configuration();
                conf.set("fs.defaultFS", hdfsPath); // HDFS cluster URL.
                try {
                    hdfs = FileSystem.get(conf);
                } catch (IOException e) {
                    e.printStackTrace();
                }
                return true;
            }

            @Override
            public void process(Row row) {
                String line = row.getString(0);
                String[] fields = line.split(",", -1);
                try {
                 //   accumulatedLines.append(newline).append("\n");
                  //  logger.fine("Processed line: " + newline);
                } catch (Exception e) {
                    logger.severe("Error in processing line: " + e.getMessage());
                }
            }

            @Override
            public void close(Throwable errorOrNull) {
                if (accumulatedLines.length() > 0) {
                    try {

                        logger.info("Writing accumulated data to HDFS");
                        Path file = new Path("/hdfs/output_" + System.currentTimeMillis() + ".txt");
                        try (FSDataOutputStream out = hdfs.create(file)) {
                            out.write(accumulatedLines.toString().getBytes());

                        }
                    } catch (Exception e) {
                        // Hata işleme.
                        logger.severe("Error writing to HDFS: " + e.getMessage());
                    }
                }

                logger.info("Closing connection");
               // redis.close();
            }
        }).start().awaitTermination();
    }
}
