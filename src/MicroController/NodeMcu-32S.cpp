#include <WiFi.h>
#include <WebServer.h>

const char* ssid = "2.4GHz";
const char* password = "";

WebServer server(80);

void printWiFiError(int errorCode) {
  Serial.print("Wi-Fi bağlantı hatası, hata kodu: ");
  Serial.println(errorCode);
}

void handleRoot() {
  server.send(200, "text/plain", "ESP32 Web Server");
}

void handleMessage() {
  if (server.hasArg("message")) {
    String message = server.arg("message");
    Serial.println("Alınan Mesaj: " + message);
    server.send(200, "text/plain", "Mesaj Alındı: " + message);
  } else {
    server.send(200, "text/plain", "Mesaj argümanı bulunamadı");
  }
}


bool connectToWiFi() {
  WiFi.begin(ssid, password);

  for (int i = 0; i < 10; i++) {
    if (WiFi.status() == WL_CONNECTED) {
      Serial.println("\nWi-Fi ağına başarıyla bağlandı.");
      Serial.print("IP Adresi: ");
      Serial.println(WiFi.localIP());
      return true;
    }
    delay(500);
    Serial.print(".");
  }

  printWiFiError(WiFi.status());
  return false;
}

void setup() {
  Serial.begin(115200);
  Serial.println("Başlatılıyor...");

  pinMode(2, OUTPUT);

  if (!connectToWiFi()) {
    Serial.println("Wi-Fi'ya bağlanılamadı, sistem yeniden başlatılacak...");
    delay(3000); // 3 saniye bekle
    ESP.restart(); // ESP32'yi yeniden başlat
  }

  server.on("/", handleRoot);
  server.on("/message", HTTP_POST, handleMessage);

  server.begin();
  Serial.println("HTTP sunucusu başlatıldı.");
}

void loop() {
  if (Serial.available() > 0) {
    String receivedString = Serial.readStringUntil('\n');

    if (receivedString.indexOf("wifiaddress") >= 0) {
      Serial.print("Mevcut IP Adresi: ");
      Serial.println(WiFi.localIP());
    }
  }

  server.handleClient();
}

