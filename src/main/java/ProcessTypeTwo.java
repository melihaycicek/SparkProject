import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.ForeachWriter;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;

import java.io.IOException;
import java.util.logging.Logger;

public class ProcessTypeTwo {

    private static final Logger logger = Logger.getLogger(ProcessTypeOne.class.getName());

    public static void process(SparkSession spark, String mainDir, String hdfsPath) throws Exception {
        spark = SparkSession.builder().appName("Structured Streaming File Processing")
                .getOrCreate();

        Dataset<Row> lines = spark.readStream()
                .format("text")
                .load(mainDir);
        logger.info("Starting to read data stream");

        lines.writeStream().foreach(new ForeachWriter<Row>() {
            private FileSystem hdfs;
            private StringBuilder accumulatedLines;

            @Override
            public boolean open(long partitionId, long version) {
                logger.info("Opening connection for partition: " + partitionId);
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
                accumulatedLines.append(line).append("\n");
                logger.fine("Processed line: " + line);
            }

            @Override
            public void close(Throwable errorOrNull) {
                if (accumulatedLines.length() > 0) {
                    try {
                        logger.info("Writing accumulated data to HDFS");
                        // "hdfsPath" parametresini kullanarak HDFS yolunu dinamik olarak oluşturun
                        Path file = new Path(hdfsPath + "/data" + System.currentTimeMillis() + ".txt");
                        try (FSDataOutputStream out = hdfs.create(file)) {
                            out.write(accumulatedLines.toString().getBytes());
                        }
                    } catch (Exception e) {
                        logger.severe("Error writing to HDFS: " + e.getMessage());
                    }
                }
                else {
                    logger.info("No data to write to HDFS");
                }
                logger.info("Closing connection");
            }
        }).start().awaitTermination();
    }
}
