
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileInputStream;
import java.io.IOException;
import java.nio.file.*;
import java.util.Properties;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class FileBearer {

    private static final Logger logger = LoggerFactory.getLogger(FileBearer.class);

    private String sourceDir;
    private String targetDirForCall;
    private String targetDirForSms;
    private ScheduledExecutorService executorService;


    public FileBearer() {
        try {
            Properties prop = new Properties();
            prop.load(new FileInputStream("SparkConfiguration.properties"));
            sourceDir = prop.getProperty("source.directory");
            targetDirForCall = prop.getProperty("target.directory.call");
            targetDirForSms = prop.getProperty("target.directory.sms");
        } catch (IOException e) {
            logger.error("Error loading configuration properties", e);
            throw new RuntimeException("Unable to load configuration properties", e);
        }

        executorService = Executors.newSingleThreadScheduledExecutor();
        logger.info("IndependentFileManager initialized");
    }

    public void startWatching() {
        executorService.scheduleAtFixedRate(this::processFiles, 0, 5, TimeUnit.SECONDS);
        logger.info("Started watching source directory: {}", sourceDir);
    }

    private void processFiles() {
        try (DirectoryStream<Path> stream = Files.newDirectoryStream(Paths.get(sourceDir))) {
            for (Path path : stream) {
                try {
                    if (path.toString().contains("Type1")) {
                        this.fileType = fileType.Type1;
                        Files.move(path, Paths.get(targetDirForTypeOne, path.getFileName().toString()));
                        logger.info("Moved a call file: {}", path);
                    } else if (path.toString().contains("Type2")) {
                        this.fileType = fileType.Type2;
                        Files.move(path, Paths.get(targetDirForTypeTwo, path.getFileName().toString()));
                        logger.info("Moved an SMS file: {}", path);
                    }
                } catch (IOException e) {
                    logger.error("Error processing file: {}", path, e);
                }
            }
        } catch (IOException e) {
            logger.error("Error reading from source directory", e);
        }
    }

    public void stopWatching() {
        executorService.shutdown();
        logger.info("Stopped watching source directory");
    }

    public static void main(String[] args) {
        try {
            FileBearer watcher = new FileBearer();
            watcher.startWatching();
        } catch (Exception e) {
            logger.error("Error in main method", e);
        }
    }
}
