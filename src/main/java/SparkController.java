import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQuery;

public class SparkController {
    public static void main(String[] args) throws Exception {

        SparkSession spark = SparkSession.builder()
                .appName("SparkController")
                .master("local") // lokal
                .config("spark.hadoop.fs.defaultFS", "hdfs://sparkprojectdocument-HdfsServerNamenode-1:8020")
                .getOrCreate();

        Dataset<Row> df = spark.readStream()
                .format("text")
                .load("file:///C:/Users/melih.aycicek/Documents/KafeinProjectDocuments/Intellimap/SparkProjectDocument/Target");


      /*  // Kafka'ya
        StreamingQuery kafkaQuery = df.writeStream()
                .format("kafka")
                .option("kafka.bootstrap.servers", "localhost:9092")
                .option("topic", "kafkaTopic")
                .option("checkpointLocation", "hdfs://HdfsServerNamenode:8020/appdata/checkpoint")
                .start();
*/
        // HDFS'ye
        StreamingQuery hdfsQuery = df.writeStream()
                .format("text")
                .option("path", "hdfs://sparkprojectdocument-HdfsServerNamenode-1:8020/appdata/outputdirectory") // HDFS yolu
                .option("checkpointLocation", "hdfs://sparkprojectdocument-HdfsServerNamenode-1:8020/appdata/checkpoint") //
                .start();

     //   kafkaQuery.awaitTermination();
        hdfsQuery.awaitTermination();
    }
}
