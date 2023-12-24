import java.net.HttpURLConnection;
import java.net.URL;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;

public class SparkController {
    public static void main(String[] args) {
        try {
            URL url = new URL("http://192.168.1.181/message");
            HttpURLConnection con = (HttpURLConnection) url.openConnection();
            con.setRequestMethod("POST");
            con.setDoOutput(true);

            String message = "Hello World";
            String postData = "message=" + URLEncoder.encode(message, StandardCharsets.UTF_8.name());

            con.getOutputStream().write(postData.getBytes(StandardCharsets.UTF_8));
            int responseCode = con.getResponseCode();
            System.out.println("POST Response Code: " + responseCode);

            con.disconnect();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}