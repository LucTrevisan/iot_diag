/*
  ============================================================================
  IoT Connectivity & Diagnostics Lab — Firmware ESP32-C3 Mini
  SENAI XR Lab · versão 1.0.0
  ============================================================================

  Compatível com o dashboard "iot-diagnostics-lab.html" (mesmo contrato JSON).

  PLACA / IDE
  - Arduino IDE 2.x ou Arduino Cloud · núcleo "esp32" da Espressif 2.0.x ou 3.x
  - Placa: "ESP32C3 Dev Module"
  - Tools → USB CDC On Boot: Enabled   (para o Serial funcionar pela USB nativa)

  BIBLIOTECAS (Gerenciador de Bibliotecas)
  - PubSubClient ............ Nick O'Leary   (MQTT)
  - WebSockets .............. Markus Sattler (WebSocketsClient)
  - ArduinoJson ............. Benoit Blanchon, versão 7.x
  O servo é acionado direto pelo periférico LEDC (sem biblioteca de servo),
  com código compatível com os núcleos 2.x e 3.x. Se houver ESP32Servo ou
  ESP32ServoController instaladas, elas não são usadas.
  O MPU6050 é lido direto pelos registradores (sem biblioteca) para permitir
  detectar 0x68/0x69, erros de I2C e valores inválidos.

  PROTOCOLO
  Apenas UM protocolo fica ativo por vez (testes separados, sem interferência).
  Troque pelo monitor serial (115200):   proto mqtt   ou   proto ws   → reboot

  CONFIGURAÇÃO PELO MONITOR SERIAL (salva na flash, sobrevive a reboot)
    ssid <nome>     pass <senha>     host <ip do PC>
    mqttport <n>    wsport <n>       user <usuario>   mqttpass <senha>
    token <token>   base <topico>    id <deviceId>    proto mqtt|ws
    sensores        show             reboot           help

  CONTRATO MQTT
    ESP32 → painel : {base}/{deviceId}/up/telemetry | status | ack | event
    painel → ESP32 : {base}/{deviceId}/down/ping | cmd
    LWT            : {base}/{deviceId}/up/status = "offline" (retain, QoS 1)

  CONTRATO WEBSOCKET
    ws://{host}:{wsport}/?role=device&deviceId=...&token=...
    O servidor Node.js repassa mensagens entre o ESP32 e os painéis pelo deviceId.

  HARDWARE — cuidados elétricos
  - ECHO do HC-SR04 sai em 5 V: use divisor 1 kΩ (série) / 2 kΩ (ao GND)
    ou conversor de nível antes do GPIO 3.
  - SG90 em fonte 5 V separada (pico ~650 mA) com GND comum ao ESP32.
  - GPIO 2, 8 e 9 são pinos de strapping do ESP32-C3 (GPIO 9 = BOOT,
    GPIO 8 costuma ter o LED da placa). NÃO são usados por este firmware.
  ============================================================================
*/

#include <Arduino.h>
#include <WiFi.h>
#include <Wire.h>
#include <Preferences.h>
#include <PubSubClient.h>
#include <WebSocketsClient.h>
#include <ArduinoJson.h>
#include <esp_task_wdt.h>
#include <esp_system.h>
#include <esp_arduino_version.h>
#include <stdarg.h>
#include <math.h>

// ============================ PINAGEM (NÃO ALTERAR) ========================
#define I2C_SCL       0
#define I2C_SDA       1

#define ULTRA_ECHO    3
#define ULTRA_TRIG    4

#define ENCODER_CLK   5
#define ENCODER_DT    6
#define ENCODER_SW    7

#define SERVO_PIN     10

// ============================ CONFIGURAÇÃO PADRÃO ==========================
// Valores usados na primeira gravação; depois podem ser trocados pelo Serial.
#define CFG_WIFI_SSID     "LAB-IOT"
#define CFG_WIFI_PASS     "senha-do-lab"
#define CFG_HOST          "192.168.0.10"     // IP do PC com Mosquitto / Node.js
#define CFG_MQTT_PORT     1883               // porta TCP do Mosquitto (não a 9001 do navegador)
#define CFG_WS_PORT       8080
#define CFG_MQTT_USER     ""
#define CFG_MQTT_PASS     ""
#define CFG_WS_TOKEN      ""
#define CFG_BASE_TOPIC    "senai/iotlab"
#define CFG_DEVICE_ID     "ESP32C3-KIT01"
#define CFG_PROTOCOL      "mqtt"             // "mqtt" ou "ws"

#define FW_VERSION        "1.0.0"

// ============================ PARÂMETROS ===================================
#define TELEMETRY_PERIOD_MS        40      // 25 Hz
#define STATUS_PERIOD_MS           1000    // 1 Hz
#define MPU_PERIOD_MS              10      // 100 Hz de leitura
#define MPU_MAX_CONSEC_ERRORS      10
#define MPU_RETRY_MS               2000
#define SONAR_PERIOD_MS            60      // >= 60 ms recomendado pelo fabricante
#define SONAR_TIMEOUT_US           25000   // ~4,3 m
#define SONAR_MIN_CM               2.0f
#define SONAR_MAX_CM               400.0f
#define ENC_STEPS_PER_DETENT       4       // KY-040 típico: 4 transições por clique (alguns: 2)
#define ENC_INVERT                 0       // 1 inverte o sentido de rotação
#define BTN_DEBOUNCE_MS            30
#define SERVO_MIN_DEG              10      // limites aplicados NO FIRMWARE
#define SERVO_MAX_DEG              170
#define SERVO_MIN_CMD_INTERVAL_MS  100     // no máximo 10 comandos/s
#define SERVO_PULSE_MIN_US         500
#define SERVO_PULSE_MAX_US         2400
#define SERVO_PWM_FREQ_HZ          50
#define SERVO_PWM_BITS             14      // ESP32-C3: até 14 bits a 50 Hz
#define SERVO_LEDC_CHANNEL         0       // usado só no núcleo 2.x
#define MQTT_KEEPALIVE_S           15
#define MQTT_BUFFER                1024
#define MAX_IN_PAYLOAD             512
#define WDT_TIMEOUT_MS             8000
#define HEAP_WARN_BYTES            30000
#define HEAP_CRITICAL_BYTES        12000
#define WIFI_CONNECT_WINDOW_MS     8000
#define WIFI_TX_POWER_REDUZIDA     1       // 1 = 8,5 dBm: corrige falhas de conexão comuns
                                           // em placas ESP32-C3 "SuperMini" (antena ruim)

// ============================ ESTADO GLOBAL ================================
struct Config {
  String ssid, pass, host, mqttUser, mqttPass, wsToken, base, deviceId, proto;
  uint16_t mqttPort, wsPort;
} cfg;

Preferences prefs;
WiFiClient netClient;
PubSubClient mqtt(netClient);
WebSocketsClient ws;

bool useMqtt = true;

char topicTelemetry[112], topicStatus[112], topicAck[112], topicEvent[112], topicDown[112];

// ---- MPU6050
uint8_t  mpuAddr = 0;
bool     mpuOk = false, mpuValid = false;
const char* mpuState = "NAO_DETECTADO";  // OK | NAO_DETECTADO | FALHA_LEITURA | TRAVADO
int16_t  mpuPrev[7] = {0};
uint16_t mpuSameCount = 0;
uint32_t mpuErrors = 0, mpuReads = 0, mpuHz = 0, mpuRetryAt = 0, mpuInitTries = 0;
uint8_t  mpuConsecErr = 0;
float    ax = 0, ay = 0, az = 0, gx = 0, gy = 0, gz = 0;
uint32_t lastMpuMs = 0;

// ---- HC-SR04 (não bloqueante, por interrupção)
volatile uint32_t sonarRise = 0, sonarFall = 0;
volatile bool     sonarDone = false;
bool     sonarWaiting = false, distValid = false;
uint32_t sonarTrigAt = 0, lastSonarMs = 0, echoUs = 0;
uint32_t sonarTimeouts = 0, sonarOutOfRange = 0;
uint8_t  sonarNoPulse = 0;               // tentativas seguidas sem NENHUM pulso no ECHO
uint32_t lastSonarValidMs = 0;
float    distanceCm = 0;

// ---- KY-040 (decodificação em quadratura por tabela)
volatile int32_t  encRaw = 0;
volatile uint32_t encInvalid = 0;
volatile uint8_t  encState = 0;
portMUX_TYPE encMux = portMUX_INITIALIZER_UNLOCKED;
static const int8_t ENC_TABLE[16] = { 0,-1, 1, 0,  1, 0, 0,-1, -1, 0, 0, 1,  0, 1,-1, 0 };
int32_t  lastDetent = 0;
uint32_t encEvents = 0;
bool     btnPressed = false, btnRaw = false;
uint32_t btnChangedAt = 0, btnPresses = 0;
bool     encDetected = false;            // resultado do teste de presença (pull-ups do módulo)
uint8_t  encProbeHist = 0;               // últimos 3 testes (bits)
uint32_t lastEncProbeMs = 0, lastEncMoveMs = 0;

// ---- Servo
bool     servoAttached = false;
int      servoCmd = -1;                 // -1 = nenhum comando recebido desde o boot
uint32_t lastServoCmdMs = 0, lastCmdSeq = 0, lastCmdSeqAt = 0;

// ---- Wi-Fi
bool     wifiUp = false, wifiEverConnected = false;
uint32_t wifiBeginAt = 0, wifiConnectMs = 0, wifiDisconnects = 0;
uint32_t wifiLostAt = 0, wifiLastReconnectMs = 0, wifiRetryAt = 0, wifiBackoff = WIFI_CONNECT_WINDOW_MS;
volatile uint8_t wifiLastReason = 0;

// ---- Link (MQTT ou WebSocket)
bool     wsConnected = false, linkWasUp = false;
uint32_t mqttRetryAt = 0, mqttBackoff = 1000;
uint32_t linkReconnects = 0, linkDownAt = 0, linkLastReconnectMs = 0;

// ---- Contadores gerais
uint32_t telemSeq = 0, pingsAnswered = 0, badPayloads = 0, loopMaxUs = 0;
uint32_t lastTelemMs = 0, lastStatusMs = 0, lastEventMs = 0;

// ============================ PROTÓTIPOS ===================================
void logEvent(const char* lvl, const char* fmt, ...);
bool linkUp();
int32_t encoderDetents();
const char* sonarState();
bool sendUp(const char* topic, JsonDocument& doc);
void handleIncoming(const uint8_t* p, size_t n, uint32_t t0us);

// ============================ UTILITÁRIOS ==================================
static inline float r1(float v) { return roundf(v * 10.0f) / 10.0f; }
static inline float r2(float v) { return roundf(v * 100.0f) / 100.0f; }
static inline float r3(float v) { return roundf(v * 1000.0f) / 1000.0f; }

static int32_t floorDiv(int32_t a, int32_t b) { return (a >= 0) ? a / b : -((-a + b - 1) / b); }

const char* resetReasonStr() {
  switch (esp_reset_reason()) {
    case ESP_RST_POWERON:   return "POWERON";
    case ESP_RST_SW:        return "SOFTWARE";
    case ESP_RST_PANIC:     return "PANIC";
    case ESP_RST_INT_WDT:   return "INT_WDT";
    case ESP_RST_TASK_WDT:  return "TASK_WDT";
    case ESP_RST_WDT:       return "WDT";
    case ESP_RST_BROWNOUT:  return "BROWNOUT";
    case ESP_RST_DEEPSLEEP: return "DEEPSLEEP";
    case ESP_RST_EXT:       return "EXTERNAL";
    default:                return "UNKNOWN";
  }
}

// ============================ CONFIGURAÇÃO (FLASH) =========================
void loadConfig() {
  prefs.begin("iotlab", true);
  cfg.ssid     = prefs.getString("ssid", CFG_WIFI_SSID);
  cfg.pass     = prefs.getString("pass", CFG_WIFI_PASS);
  cfg.host     = prefs.getString("host", CFG_HOST);
  cfg.mqttUser = prefs.getString("user", CFG_MQTT_USER);
  cfg.mqttPass = prefs.getString("mqttpass", CFG_MQTT_PASS);
  cfg.wsToken  = prefs.getString("token", CFG_WS_TOKEN);
  cfg.base     = prefs.getString("base", CFG_BASE_TOPIC);
  cfg.deviceId = prefs.getString("id", CFG_DEVICE_ID);
  cfg.proto    = prefs.getString("proto", CFG_PROTOCOL);
  cfg.mqttPort = prefs.getUShort("mqttport", CFG_MQTT_PORT);
  cfg.wsPort   = prefs.getUShort("wsport", CFG_WS_PORT);
  prefs.end();
}

void printConfig() {
  Serial.println(F("---- Configuração atual ----"));
  Serial.printf("  deviceId : %s\n", cfg.deviceId.c_str());
  Serial.printf("  protocolo: %s\n", cfg.proto.c_str());
  Serial.printf("  ssid     : %s\n", cfg.ssid.c_str());
  Serial.printf("  pass     : %s\n", cfg.pass.length() ? "********" : "(vazia)");
  Serial.printf("  host     : %s  (MQTT %u · WS %u)\n", cfg.host.c_str(), cfg.mqttPort, cfg.wsPort);
  Serial.printf("  usuário  : %s  senha MQTT: %s  token WS: %s\n", cfg.mqttUser.c_str(),
                cfg.mqttPass.length() ? "****" : "(vazia)", cfg.wsToken.length() ? "****" : "(vazio)");
  Serial.printf("  base     : %s\n", cfg.base.c_str());
  Serial.println(F("  Pinos    : SDA=1 SCL=0 · TRIG=4 ECHO=3 · CLK=5 DT=6 SW=7 · SERVO=10"));
}

void runSerialCommand(String line) {
  line.trim();
  int sp = line.indexOf(' ');
  String key = (sp < 0 ? line : line.substring(0, sp));
  String val = (sp < 0 ? String("") : line.substring(sp + 1));
  key.toLowerCase();
  val.trim();

  if (key == "help") {
    Serial.println(F("Comandos: sensores show reboot · ssid pass host mqttport wsport user mqttpass token base id proto"));
    return;
  }
  if (key == "show")   { printConfig(); return; }
  if (key == "sensores") { printSensorReport(); return; }
  if (key == "reboot") { Serial.println(F("Reiniciando...")); delay(200); ESP.restart(); }

  const char* strKeys[] = { "ssid", "pass", "host", "user", "mqttpass", "token", "base", "id", "proto" };
  for (const char* k : strKeys) {
    if (key == k) {
      if (key == "proto" && val != "mqtt" && val != "ws") { Serial.println(F("Use: proto mqtt  ou  proto ws")); return; }
      if ((key == "id" || key == "base") && (val.length() == 0 || val.length() > 48 || val.indexOf('#') >= 0 || val.indexOf('+') >= 0)) {
        Serial.println(F("Valor inválido (1–48 caracteres, sem # ou +)")); return;
      }
      prefs.begin("iotlab", false); prefs.putString(k, val); prefs.end();
      Serial.printf("'%s' salvo. Digite 'reboot' para aplicar.\n", k);
      return;
    }
  }
  if (key == "mqttport" || key == "wsport") {
    long p = val.toInt();
    if (p < 1 || p > 65535) { Serial.println(F("Porta inválida")); return; }
    prefs.begin("iotlab", false); prefs.putUShort(key.c_str(), (uint16_t)p); prefs.end();
    Serial.printf("'%s' = %ld salvo. Digite 'reboot' para aplicar.\n", key.c_str(), p);
    return;
  }
  Serial.println(F("Comando desconhecido. Digite 'help'."));
}

void serialTask() {
  static String line;
  while (Serial.available()) {
    char c = (char)Serial.read();
    if (c == '\n' || c == '\r') { if (line.length()) runSerialCommand(line); line = ""; }
    else if (line.length() < 128) line += c;
  }
}

// ============================ LOG ESTRUTURADO ==============================
void logEvent(const char* lvl, const char* fmt, ...) {
  char msg[160];
  va_list ap; va_start(ap, fmt); vsnprintf(msg, sizeof(msg), fmt, ap); va_end(ap);
  for (char* p = msg; *p; p++) if (*p == '"') *p = '\'';
  Serial.printf("{\"t\":%lu,\"lvl\":\"%s\",\"msg\":\"%s\"}\n", (unsigned long)millis(), lvl, msg);
  // Também publica para o painel, com limite de frequência (máx. 10/s)
  if (linkUp() && millis() - lastEventMs >= 100) {
    lastEventMs = millis();
    JsonDocument d;
    d["deviceId"] = cfg.deviceId;
    d["type"] = "event";
    d["level"] = lvl;
    d["msg"] = msg;
    d["timestamp"] = millis();
    sendUp(topicEvent, d);
  }
}

// ============================ RELATÓRIO DE SENSORES (SERIAL) ===============
void printSensorReport() {
  Serial.println(F("\n======== VERIFICAÇÃO DOS SENSORES ========"));
  Serial.printf("MPU6050  (SDA 1, SCL 0) : %-14s", mpuState);
  if (mpuOk) Serial.printf(" end. 0x%02X · %lu Hz · a=(%.2f %.2f %.2f) g", mpuAddr, (unsigned long)mpuHz, ax, ay, az);
  Serial.println();
  Serial.printf("HC-SR04  (TRIG 4, ECHO 3): %-14s", sonarState());
  if (distValid) Serial.printf(" %.1f cm · eco %lu us", distanceCm, (unsigned long)echoUs);
  Serial.printf(" · timeouts %lu\n", (unsigned long)sonarTimeouts);
  Serial.printf("KY-040   (CLK 5, DT 6)   : %-14s contagem %ld · %s\n", encDetected ? "DETECTADO" : "NAO_DETECTADO",
                (long)encoderDetents(), (lastEncMoveMs && millis() - lastEncMoveMs < 10000) ? "girando" : "parado");
  Serial.printf("KY-040   botão (SW 7)    : %-14s %lu acionamentos (pressione para confirmar)\n",
                btnPressed ? "PRESSIONADO" : "SOLTO", (unsigned long)btnPresses);
  Serial.printf("SG90     (PWM 10)        : %-14s sem realimentação — confirme visualmente\n",
                servoAttached ? "PWM_ATIVO" : "PWM_DESLIGADO");
  Serial.printf("Wi-Fi: %s · link %s\n", wifiUp ? WiFi.localIP().toString().c_str() : "desconectado",
                linkUp() ? (useMqtt ? "MQTT ok" : "WebSocket ok") : "offline");
  Serial.println(F("==========================================="));
}

// ============================ WATCHDOG =====================================
void wdtInit() {
#if ESP_ARDUINO_VERSION_MAJOR >= 3
  esp_task_wdt_config_t c = { .timeout_ms = WDT_TIMEOUT_MS, .idle_core_mask = 0, .trigger_panic = true };
  if (esp_task_wdt_reconfigure(&c) != ESP_OK) esp_task_wdt_init(&c);
#else
  esp_task_wdt_init(WDT_TIMEOUT_MS / 1000, true);
#endif
  esp_task_wdt_add(NULL);
}

// ============================ MPU6050 ======================================
bool i2cWrite(uint8_t addr, uint8_t reg, uint8_t val) {
  Wire.beginTransmission(addr); Wire.write(reg); Wire.write(val);
  return Wire.endTransmission() == 0;
}

int i2cRead8(uint8_t addr, uint8_t reg) {
  Wire.beginTransmission(addr); Wire.write(reg);
  if (Wire.endTransmission(false) != 0) return -1;
  if (Wire.requestFrom(addr, (uint8_t)1) != 1) return -1;
  return Wire.read();
}

bool mpuInit(bool verbose) {
  mpuOk = false; mpuValid = false; mpuInitTries++;
  const uint8_t addrs[2] = { 0x68, 0x69 };
  for (uint8_t a : addrs) {
    Wire.beginTransmission(a);
    if (Wire.endTransmission() != 0) continue;
    int who = i2cRead8(a, 0x75);
    if (who < 0) continue;
    if (who != 0x68 && verbose)
      logEvent("warn", "MPU6050 em 0x%02X com WHO_AM_I=0x%02X (possível clone/MPU6500)", a, who);
    bool ok = i2cWrite(a, 0x6B, 0x00);     // sai do sleep, clock interno
    delay(20);
    ok = ok && i2cWrite(a, 0x19, 0x09);    // divisor de amostragem → 100 Hz
    ok = ok && i2cWrite(a, 0x1A, 0x03);    // DLPF ~44 Hz
    ok = ok && i2cWrite(a, 0x1B, 0x08);    // giroscópio ±500 °/s
    ok = ok && i2cWrite(a, 0x1C, 0x00);    // acelerômetro ±2 g
    if (ok) {
      mpuAddr = a; mpuOk = true; mpuConsecErr = 0; mpuSameCount = 0; mpuState = "OK";
      logEvent("ok", "MPU6050 detectado em 0x%02X (SDA=GPIO%d, SCL=GPIO%d)", a, I2C_SDA, I2C_SCL);
      return true;
    }
  }
  mpuState = "NAO_DETECTADO";
  if (verbose || mpuInitTries % 15 == 0)
    logEvent("err", "MPU6050 não detectado em 0x68/0x69 — verifique SDA=GPIO1, SCL=GPIO0, 3V3 e GND");
  return false;
}

void mpuFail() {
  mpuErrors++;
  if (++mpuConsecErr >= MPU_MAX_CONSEC_ERRORS && mpuOk) {
    mpuOk = false; mpuValid = false; mpuState = "FALHA_LEITURA";
    mpuRetryAt = millis() + MPU_RETRY_MS;
    logEvent("err", "MPU6050: comunicação interrompida após %d erros consecutivos", MPU_MAX_CONSEC_ERRORS);
  }
}

void mpuRead() {
  Wire.beginTransmission(mpuAddr); Wire.write(0x3B);
  if (Wire.endTransmission(false) != 0) { mpuFail(); return; }
  if (Wire.requestFrom(mpuAddr, (uint8_t)14) != 14) { mpuFail(); return; }
  int16_t v[7];
  bool allZero = true, allFF = true;
  for (int i = 0; i < 7; i++) {
    uint8_t hi = Wire.read();
    uint8_t lo = Wire.read();
    v[i] = (int16_t)((hi << 8) | lo);
    if (v[i] != 0) allZero = false;
    if (v[i] != -1) allFF = false;
  }
  if (allZero || allFF) { mpuFail(); return; }   // barramento travado ou sensor sem energia
  // Sensor real sempre tem ruído: 100 leituras idênticas (1 s) = sensor congelado
  bool same = memcmp(v, mpuPrev, sizeof(v)) == 0;
  memcpy(mpuPrev, v, sizeof(v));
  if (same) { if (mpuSameCount < 1000) mpuSameCount++; } else mpuSameCount = 0;
  if (mpuSameCount >= 100) {
    if (strcmp(mpuState, "TRAVADO") != 0) logEvent("warn", "MPU6050: valores congelados — sensor responde mas não atualiza");
    mpuState = "TRAVADO";
  } else mpuState = "OK";
  ax = v[0] / 16384.0f; ay = v[1] / 16384.0f; az = v[2] / 16384.0f;
  gx = v[4] / 65.5f;    gy = v[5] / 65.5f;    gz = v[6] / 65.5f;
  if (!isfinite(ax) || !isfinite(gx)) { mpuFail(); return; }
  mpuConsecErr = 0; mpuReads++; mpuValid = true;
}

void mpuTask() {
  if (!mpuOk) {
    if (millis() >= mpuRetryAt) { mpuRetryAt = millis() + MPU_RETRY_MS; mpuInit(false); }
    return;
  }
  if (millis() - lastMpuMs >= MPU_PERIOD_MS) { lastMpuMs = millis(); mpuRead(); }
}

// ============================ HC-SR04 ======================================
void IRAM_ATTR sonarIsr() {
  uint32_t t = micros();
  if (digitalRead(ULTRA_ECHO)) sonarRise = t;
  else if (sonarRise) { sonarFall = t; sonarDone = true; }
}

void sonarTask() {
  if (sonarWaiting) {
    if (sonarDone) {
      sonarWaiting = false;
      uint32_t dt = sonarFall - sonarRise;
      echoUs = dt;
      distanceCm = dt / 58.2f;
      distValid = true;
      if (sonarNoPulse >= 5) logEvent("ok", "HC-SR04 voltou a responder");
      sonarNoPulse = 0;
      if (distanceCm < SONAR_MIN_CM || distanceCm > SONAR_MAX_CM) sonarOutOfRange++;
      else lastSonarValidMs = millis();
    } else if (micros() - sonarTrigAt > SONAR_TIMEOUT_US) {
      sonarWaiting = false;
      distValid = false;
      sonarTimeouts++;
      if (sonarRise == 0) {                // nem começou o pulso: sensor ausente / sem 5 V / fio solto
        if (sonarNoPulse < 255) sonarNoPulse++;
        if (sonarNoPulse == 5) logEvent("err", "HC-SR04 não responde (sem pulso no ECHO, GPIO 3)");
      } else sonarNoPulse = 0;             // houve pulso, mas sem eco: sensor presente, sem alvo
    }
    return;
  }
  if (millis() - lastSonarMs >= SONAR_PERIOD_MS) {
    lastSonarMs = millis();
    noInterrupts(); sonarRise = 0; sonarFall = 0; sonarDone = false; interrupts();
    digitalWrite(ULTRA_TRIG, HIGH);
    delayMicroseconds(10);
    digitalWrite(ULTRA_TRIG, LOW);
    sonarTrigAt = micros();
    sonarWaiting = true;
  }
}

const char* sonarState() {
  if (sonarNoPulse >= 5) return "SEM_RESPOSTA";
  if (lastSonarValidMs && millis() - lastSonarValidMs < 1000) return "OK";
  return "SEM_ALVO";
}

// ============================ KY-040 =======================================
void IRAM_ATTR encIsr() {
  uint8_t s = (uint8_t)((digitalRead(ENCODER_CLK) << 1) | digitalRead(ENCODER_DT));
  portENTER_CRITICAL_ISR(&encMux);
  uint8_t idx = (uint8_t)((encState << 2) | s);
  int8_t d = ENC_TABLE[idx];
  if (d == 0 && s != encState) encInvalid++;   // os dois canais mudaram ao mesmo tempo
  encRaw += d;
  encState = s;
  portEXIT_CRITICAL_ISR(&encMux);
}

// Teste de presença do KY-040: o módulo tem resistores de pull-up (10 kΩ) em CLK e DT
// ligados ao pino "+". Com o pull-down interno ativado por alguns µs, uma linha que
// continua em nível alto prova que o módulo está ligado. Sem módulo, as linhas caem.
void encoderProbe() {
  detachInterrupt(digitalPinToInterrupt(ENCODER_CLK));
  detachInterrupt(digitalPinToInterrupt(ENCODER_DT));
  pinMode(ENCODER_CLK, INPUT_PULLDOWN);
  pinMode(ENCODER_DT, INPUT_PULLDOWN);
  delayMicroseconds(60);
  bool hit = digitalRead(ENCODER_CLK) || digitalRead(ENCODER_DT);
  pinMode(ENCODER_CLK, INPUT_PULLUP);
  pinMode(ENCODER_DT, INPUT_PULLUP);
  delayMicroseconds(30);
  portENTER_CRITICAL(&encMux);
  encState = (uint8_t)((digitalRead(ENCODER_CLK) << 1) | digitalRead(ENCODER_DT));
  portEXIT_CRITICAL(&encMux);
  attachInterrupt(digitalPinToInterrupt(ENCODER_CLK), encIsr, CHANGE);
  attachInterrupt(digitalPinToInterrupt(ENCODER_DT), encIsr, CHANGE);

  encProbeHist = (uint8_t)(((encProbeHist << 1) | (hit ? 1 : 0)) & 0x07);
  bool det = encProbeHist != 0 || (lastEncMoveMs && millis() - lastEncMoveMs < 10000);
  if (det != encDetected) {
    encDetected = det;
    logEvent(det ? "ok" : "err", det ? "KY-040 detectado (pull-ups em CLK/DT)"
                                     : "KY-040 não detectado — verifique + (3V3), GND, CLK=GPIO5, DT=GPIO6");
  }
}

int32_t encoderDetents() {
  portENTER_CRITICAL(&encMux);
  int32_t raw = encRaw;
  portEXIT_CRITICAL(&encMux);
  int32_t c = floorDiv(raw, ENC_STEPS_PER_DETENT);
  return ENC_INVERT ? -c : c;
}

void encoderTask() {
  int32_t c = encoderDetents();
  if (c != lastDetent) { encEvents += (uint32_t)abs(c - lastDetent); lastDetent = c; lastEncMoveMs = millis(); }
  if (millis() - lastEncProbeMs >= 5000 && millis() - lastEncMoveMs > 500) { lastEncProbeMs = millis(); encoderProbe(); }

  bool raw = digitalRead(ENCODER_SW) == LOW;   // ativo em nível baixo
  if (raw != btnRaw) { btnRaw = raw; btnChangedAt = millis(); }
  if (millis() - btnChangedAt >= BTN_DEBOUNCE_MS && btnPressed != btnRaw) {
    btnPressed = btnRaw;
    if (btnPressed) btnPresses++;
  }
}

// ============================ SERVO ========================================
// PWM do servo via LEDC — mesma API para os núcleos 2.x e 3.x
bool servoAttach() {
#if ESP_ARDUINO_VERSION_MAJOR >= 3
  if (!ledcAttach(SERVO_PIN, SERVO_PWM_FREQ_HZ, SERVO_PWM_BITS)) return false;
#else
  if (ledcSetup(SERVO_LEDC_CHANNEL, SERVO_PWM_FREQ_HZ, SERVO_PWM_BITS) == 0) return false;
  ledcAttachPin(SERVO_PIN, SERVO_LEDC_CHANNEL);
#endif
  return true;
}

void servoDetach() {
#if ESP_ARDUINO_VERSION_MAJOR >= 3
  ledcDetach(SERVO_PIN);
#else
  ledcDetachPin(SERVO_PIN);
#endif
  pinMode(SERVO_PIN, OUTPUT);
  digitalWrite(SERVO_PIN, LOW);           // sem pulso: o SG90 não recebe comando de posição
}

void servoWriteDeg(int deg) {
  uint32_t us = SERVO_PULSE_MIN_US + (uint32_t)((SERVO_PULSE_MAX_US - SERVO_PULSE_MIN_US) * (long)deg / 180L);
  uint32_t period = 1000000UL / SERVO_PWM_FREQ_HZ;                 // 20 000 µs
  uint32_t duty = (uint32_t)(((uint64_t)us * ((1UL << SERVO_PWM_BITS) - 1)) / period);
#if ESP_ARDUINO_VERSION_MAJOR >= 3
  ledcWrite(SERVO_PIN, duty);
#else
  ledcWrite(SERVO_LEDC_CHANNEL, duty);
#endif
}

void servoSafe(const char* why) {
  if (servoAttached) { servoDetach(); servoAttached = false; }
  logEvent("warn", "Servo em estado seguro (PWM desligado, sem movimento): %s", why);
}

void sendCmdAck(uint32_t seq, const char* status, const char* reason, uint32_t t0us) {
  JsonDocument r;
  r["type"] = "cmdAck";
  r["deviceId"] = cfg.deviceId;
  r["sequence"] = seq;
  r["status"] = status;
  if (reason) r["reason"] = reason;
  if (servoCmd >= 0) r["angle"] = servoCmd; else r["angle"] = nullptr;
  r["procUs"] = micros() - t0us;
  sendUp(topicAck, r);
}

void handleCmd(JsonDocument& in, uint32_t t0us) {
  if (!in["sequence"].is<uint32_t>()) { badPayloads++; return; }
  uint32_t seq = in["sequence"].as<uint32_t>();
  const char* cmd = in["cmd"] | "";

  // Reentrega (ex.: QoS 1 duplicado): confirma sem repetir a ação
  if (seq == lastCmdSeq && millis() - lastCmdSeqAt < 5000) { sendCmdAck(seq, "duplicate", "comando já processado", t0us); return; }
  lastCmdSeq = seq; lastCmdSeqAt = millis();

  if (strcmp(cmd, "safe") == 0) { servoSafe("solicitado pelo painel"); sendCmdAck(seq, "safe", nullptr, t0us); return; }
  if (strcmp(cmd, "servo") != 0) { sendCmdAck(seq, "rejected", "comando desconhecido", t0us); return; }
  if (!in["angle"].is<float>()) { sendCmdAck(seq, "rejected", "ângulo ausente", t0us); return; }

  float a = in["angle"].as<float>();
  if (!isfinite(a) || a < SERVO_MIN_DEG || a > SERVO_MAX_DEG) {
    sendCmdAck(seq, "rejected", "fora dos limites do firmware (10–170°)", t0us); return;
  }
  if (lastServoCmdMs && millis() - lastServoCmdMs < SERVO_MIN_CMD_INTERVAL_MS) {
    sendCmdAck(seq, "rejected", "limite de frequência (100 ms)", t0us); return;
  }

  // O servo só recebe PWM após o primeiro comando válido: nada se move no boot.
  int target = (int)lroundf(a);
  if (!servoAttached) {                   // ao ligar, o LEDC começa com duty 0 (sem pulso)
    if (!servoAttach()) { sendCmdAck(seq, "rejected", "falha ao configurar PWM (LEDC)", t0us); return; }
    servoAttached = true;
  }
  servoCmd = target;
  servoWriteDeg(servoCmd);
  lastServoCmdMs = millis();
  sendCmdAck(seq, "ok", nullptr, t0us);   // "ok" = PWM aplicado; posição física NÃO é medida
}

// ============================ MENSAGENS RECEBIDAS ==========================
void handleIncoming(const uint8_t* p, size_t n, uint32_t t0us) {
  if (n == 0 || n > MAX_IN_PAYLOAD) { badPayloads++; return; }
  JsonDocument in;
  if (deserializeJson(in, p, n)) { badPayloads++; return; }   // copia o payload: é seguro publicar depois

  const char* type = in["type"] | "";
  if (strcmp(type, "heartbeat") == 0 || strcmp(type, "heartbeatAck") == 0 || strcmp(type, "hello") == 0) return;

  const char* dev = in["deviceId"] | "";
  if (strcmp(dev, cfg.deviceId.c_str()) != 0) return;   // mensagem para outro kit

  if (strcmp(type, "ping") == 0) {
    if (!in["sequence"].is<uint32_t>()) { badPayloads++; return; }
    JsonDocument a;
    a["type"] = "ack";
    a["deviceId"] = cfg.deviceId;
    a["testId"] = in["testId"];
    a["sequence"] = in["sequence"];
    a["procUs"] = micros() - t0us;          // tempo de processamento no ESP32
    if (sendUp(topicAck, a)) pingsAnswered++;
    return;
  }
  if (strcmp(type, "cmd") == 0) {
    if (!linkUp()) return;
    handleCmd(in, t0us);
    return;
  }
  badPayloads++;
}

// ============================ ENVIO ========================================
bool linkUp() {
  if (WiFi.status() != WL_CONNECTED) return false;
  return useMqtt ? mqtt.connected() : wsConnected;
}

bool sendUp(const char* topic, JsonDocument& doc) {
  if (!linkUp()) return false;
  static char buf[800];
  size_t n = serializeJson(doc, buf, sizeof(buf));
  if (n == 0 || n >= sizeof(buf) - 1) return false;
  if (useMqtt) return mqtt.publish(topic, (const uint8_t*)buf, n, false);   // PubSubClient publica em QoS 0
  return ws.sendTXT(buf, n);
}

void sendTelemetry() {
  JsonDocument d;
  d["deviceId"] = cfg.deviceId;
  d["sequence"] = ++telemSeq;
  d["timestamp"] = millis();               // millis() do ESP32 (rede offline, sem NTP)
  d["type"] = "telemetry";

  JsonObject s = d["sensors"].to<JsonObject>();
  if (distValid) { s["distance"] = r1(distanceCm); s["echoUs"] = echoUs; }
  else           { s["distance"] = nullptr;        s["echoUs"] = nullptr; }
  if (mpuValid) {
    s["accelX"] = r3(ax); s["accelY"] = r3(ay); s["accelZ"] = r3(az);
    s["gyroX"]  = r2(gx); s["gyroY"]  = r2(gy); s["gyroZ"]  = r2(gz);
  } else {
    s["accelX"] = nullptr; s["accelY"] = nullptr; s["accelZ"] = nullptr;
    s["gyroX"]  = nullptr; s["gyroY"]  = nullptr; s["gyroZ"]  = nullptr;
  }
  s["encoder"] = encoderDetents();
  s["encoderBtn"] = btnPressed;
  if (servoCmd >= 0) s["servoCommand"] = servoCmd; else s["servoCommand"] = nullptr;

  JsonObject g = d["diag"].to<JsonObject>();
  g["mpuOk"] = mpuOk;
  if (mpuOk) { char a[6]; snprintf(a, sizeof(a), "0x%02X", mpuAddr); g["mpuAddr"] = a; }
  else g["mpuAddr"] = nullptr;
  g["mpuHz"] = mpuHz;
  g["mpuErrors"] = mpuErrors;
  g["sonarTimeouts"] = sonarTimeouts;
  g["sonarOutOfRange"] = sonarOutOfRange;
  g["encoderInvalid"] = encInvalid;
  g["encoderEvents"] = encEvents;
  g["buttonPresses"] = btnPresses;
  g["servoAttached"] = servoAttached;

  JsonObject pr = g["presence"].to<JsonObject>();
  pr["mpu"] = mpuState;
  pr["sonar"] = sonarState();
  pr["encoder"] = encDetected ? "DETECTADO" : "NAO_DETECTADO";
  pr["encoderActive"] = lastEncMoveMs && millis() - lastEncMoveMs < 10000;
  pr["buttonSeen"] = btnPresses > 0;
  pr["servoPwm"] = servoAttached;

  if (!sendUp(topicTelemetry, d)) telemSeq--;   // não conta como enviada
}

void sendStatus() {
  JsonDocument d;
  d["deviceId"] = cfg.deviceId;
  d["type"] = "status";
  d["uptimeMs"] = millis();
  d["freeHeap"] = ESP.getFreeHeap();
  d["minHeap"] = ESP.getMinFreeHeap();
  d["resetReason"] = resetReasonStr();
  d["fw"] = FW_VERSION;

  JsonObject w = d["wifi"].to<JsonObject>();
  w["ssid"] = WiFi.SSID();
  w["ip"] = WiFi.localIP().toString();
  w["mac"] = WiFi.macAddress();
  w["gateway"] = WiFi.gatewayIP().toString();
  w["rssi"] = WiFi.RSSI();
  w["channel"] = WiFi.channel();
  w["connectMs"] = wifiConnectMs;
  w["disconnects"] = wifiDisconnects;
  w["reconnectMs"] = wifiLastReconnectMs;
  w["lastReason"] = wifiLastReason;

  JsonObject l = d["link"].to<JsonObject>();
  l["proto"] = useMqtt ? "mqtt" : "ws";
  l["reconnects"] = linkReconnects;
  l["lastReconnectMs"] = linkLastReconnectMs;
  l["pingsAnswered"] = pingsAnswered;
  l["badPayloads"] = badPayloads;
  l["loopMaxUs"] = loopMaxUs;

  sendUp(topicStatus, d);
}

// ============================ WI-FI ========================================
void wifiStart() {
  WiFi.disconnect(true);
  WiFi.begin(cfg.ssid.c_str(), cfg.pass.c_str());
#if WIFI_TX_POWER_REDUZIDA
  WiFi.setTxPower(WIFI_POWER_8_5dBm);
#endif
}

void wifiTask() {
  bool up = WiFi.status() == WL_CONNECTED;
  if (up && !wifiUp) {
    wifiUp = true;
    if (!wifiEverConnected) { wifiConnectMs = millis() - wifiBeginAt; wifiEverConnected = true; }
    else wifiLastReconnectMs = millis() - wifiLostAt;
    wifiBackoff = WIFI_CONNECT_WINDOW_MS;
    logEvent("ok", "Wi-Fi conectado: %s · IP %s · RSSI %d dBm · %lu ms",
             WiFi.SSID().c_str(), WiFi.localIP().toString().c_str(), WiFi.RSSI(),
             (unsigned long)(wifiDisconnects ? wifiLastReconnectMs : wifiConnectMs));
  } else if (!up && wifiUp) {
    wifiUp = false;
    wifiDisconnects++;
    wifiLostAt = millis();
    wifiRetryAt = millis() + 500;
    logEvent("warn", "Wi-Fi desconectado (motivo %u) — reconectando", wifiLastReason);
  } else if (!up && millis() >= wifiRetryAt) {
    Serial.printf("[wifi] tentando conectar em '%s' (próxima janela %lu ms)\n", cfg.ssid.c_str(), (unsigned long)wifiBackoff);
    wifiStart();
    wifiRetryAt = millis() + wifiBackoff;
    wifiBackoff = min<uint32_t>(wifiBackoff * 2, 30000);   // backoff exponencial
  }
}

// ============================ MQTT =========================================
void mqttCallback(char* topic, byte* payload, unsigned int len) {
  uint32_t t0 = micros();
  (void)topic;
  handleIncoming((const uint8_t*)payload, len, t0);
}

void mqttTask() {
  if (!wifiUp) return;
  if (mqtt.connected()) { mqtt.loop(); return; }
  if (millis() < mqttRetryAt) return;

  esp_task_wdt_reset();
  String cid = cfg.deviceId + "-" + String((uint32_t)ESP.getEfuseMac(), HEX);
  bool ok;
  if (cfg.mqttUser.length())
    ok = mqtt.connect(cid.c_str(), cfg.mqttUser.c_str(), cfg.mqttPass.c_str(), topicStatus, 1, true, "offline");
  else
    ok = mqtt.connect(cid.c_str(), topicStatus, 1, true, "offline");

  if (ok) {
    mqttBackoff = 1000;
    mqtt.publish(topicStatus, "online", true);
    mqtt.subscribe(topicDown, 1);
    logEvent("ok", "MQTT conectado a %s:%u · inscrito em %s", cfg.host.c_str(), cfg.mqttPort, topicDown);
  } else {
    int st = mqtt.state();
    const char* why =
      st == -4 ? "timeout: o broker não respondeu ao CONNECT" :
      st == -2 ? "falha TCP: não abriu conexão com host:porta (IP errado, Mosquitto parado, só ouvindo localhost ou firewall)" :
      st == 1  ? "versão de protocolo recusada" :
      st == 2  ? "client id recusado" :
      st == 3  ? "broker indisponível" :
      st == 4  ? "usuário/senha inválidos" :
      st == 5  ? "não autorizado (allow_anonymous false?)" : "erro desconhecido";
    Serial.printf("[mqtt] falha ao conectar em %s:%u (estado %d: %s) — nova tentativa em %lu ms\n",
                  cfg.host.c_str(), cfg.mqttPort, st, why, (unsigned long)mqttBackoff);
    mqttRetryAt = millis() + mqttBackoff + random(0, 300);
    mqttBackoff = min<uint32_t>(mqttBackoff * 2, 10000);   // laboratório: no máximo 10 s entre tentativas
  }
}

// ============================ WEBSOCKET ====================================
void wsEvent(WStype_t type, uint8_t* payload, size_t len) {
  uint32_t t0 = micros();
  switch (type) {
    case WStype_CONNECTED: {
      wsConnected = true;
      JsonDocument h;
      h["type"] = "hello";
      h["role"] = "device";
      h["deviceId"] = cfg.deviceId;
      h["fw"] = FW_VERSION;
      if (cfg.wsToken.length()) h["token"] = cfg.wsToken;
      sendUp("", h);
      logEvent("ok", "WebSocket conectado a %s:%u", cfg.host.c_str(), cfg.wsPort);
      break;
    }
    case WStype_DISCONNECTED:
      if (wsConnected) Serial.println(F("[ws] desconectado"));
      wsConnected = false;
      break;
    case WStype_TEXT:
      handleIncoming(payload, len, t0);
      break;
    default:
      break;
  }
}

// ============================ ESTADO DO LINK ===============================
void linkTrack() {
  bool up = linkUp();
  if (up && !linkWasUp) {
    if (linkDownAt) {
      linkLastReconnectMs = millis() - linkDownAt;
      linkReconnects++;
      logEvent("ok", "Link %s restabelecido em %lu ms", useMqtt ? "MQTT" : "WebSocket", (unsigned long)linkLastReconnectMs);
    }
    linkDownAt = 0;
  } else if (!up && linkWasUp) {
    linkDownAt = millis();
    // Estado seguro: o servo mantém a última posição e nenhum comando novo é aceito
    Serial.println(F("{\"lvl\":\"warn\",\"msg\":\"Link perdido — servo mantém posição, comandos bloqueados\"}"));
  }
  linkWasUp = up;
}

// ============================ SETUP / LOOP =================================
void buildTopics() {
  const char* b = cfg.base.c_str(); const char* id = cfg.deviceId.c_str();
  snprintf(topicTelemetry, sizeof(topicTelemetry), "%s/%s/up/telemetry", b, id);
  snprintf(topicStatus,    sizeof(topicStatus),    "%s/%s/up/status",    b, id);
  snprintf(topicAck,       sizeof(topicAck),       "%s/%s/up/ack",       b, id);
  snprintf(topicEvent,     sizeof(topicEvent),     "%s/%s/up/event",     b, id);
  snprintf(topicDown,      sizeof(topicDown),      "%s/%s/down/#",       b, id);
}

void setup() {
  Serial.begin(115200);
  delay(400);
  Serial.println(F("\n=== IoT Connectivity & Diagnostics Lab — ESP32-C3 · fw " FW_VERSION " ==="));
  loadConfig();
  useMqtt = (cfg.proto != "ws");
  buildTopics();
  printConfig();
  Serial.printf("Motivo do reset: %s\n", resetReasonStr());

  // HC-SR04
  pinMode(ULTRA_TRIG, OUTPUT);
  digitalWrite(ULTRA_TRIG, LOW);
  pinMode(ULTRA_ECHO, INPUT);                 // ECHO via divisor de tensão 5 V → 3,3 V
  attachInterrupt(digitalPinToInterrupt(ULTRA_ECHO), sonarIsr, CHANGE);

  // KY-040
  pinMode(ENCODER_CLK, INPUT_PULLUP);
  pinMode(ENCODER_DT, INPUT_PULLUP);
  pinMode(ENCODER_SW, INPUT_PULLUP);
  encState = (uint8_t)((digitalRead(ENCODER_CLK) << 1) | digitalRead(ENCODER_DT));
  attachInterrupt(digitalPinToInterrupt(ENCODER_CLK), encIsr, CHANGE);
  attachInterrupt(digitalPinToInterrupt(ENCODER_DT), encIsr, CHANGE);
  encoderProbe();

  // MPU6050
  Wire.begin(I2C_SDA, I2C_SCL);
  Wire.setClock(400000);
  Wire.setTimeOut(20);
  mpuInit(true);

  // Servo: PWM só é ligado no primeiro comando válido (sem movimento no boot)

  // Wi-Fi
  WiFi.onEvent([](WiFiEvent_t e, WiFiEventInfo_t info) {
    if (e == ARDUINO_EVENT_WIFI_STA_DISCONNECTED) wifiLastReason = info.wifi_sta_disconnected.reason;
  });
  WiFi.mode(WIFI_STA);
  WiFi.setSleep(false);                       // menor latência
  WiFi.setAutoReconnect(false);               // reconexão controlada pelo firmware (backoff)
  String host = cfg.deviceId; host.toLowerCase();
  WiFi.setHostname(host.c_str());
  wifiBeginAt = millis();
  wifiStart();
  wifiRetryAt = millis() + WIFI_CONNECT_WINDOW_MS;

  if (useMqtt) {
    mqtt.setServer(cfg.host.c_str(), cfg.mqttPort);
    mqtt.setCallback(mqttCallback);
    mqtt.setBufferSize(MQTT_BUFFER);
    mqtt.setKeepAlive(MQTT_KEEPALIVE_S);
    mqtt.setSocketTimeout(3);
  } else {
    String path = "/?role=device&deviceId=" + cfg.deviceId;
    if (cfg.wsToken.length()) path += "&token=" + cfg.wsToken;
    ws.begin(cfg.host.c_str(), cfg.wsPort, path.c_str());
    ws.onEvent(wsEvent);
    ws.setReconnectInterval(2000);
    ws.enableHeartbeat(5000, 3000, 2);        // ping/pong: 3 s de espera, 2 falhas → reconecta
  }

  wdtInit();
  logEvent("info", "Firmware iniciado · protocolo %s · deviceId %s", useMqtt ? "MQTT" : "WebSocket", cfg.deviceId.c_str());
}

void loop() {
  uint32_t t0 = micros();
  esp_task_wdt_reset();

  serialTask();
  wifiTask();
  if (useMqtt) mqttTask();
  else if (wifiUp) { esp_task_wdt_reset(); ws.loop(); }
  linkTrack();

  mpuTask();
  sonarTask();
  encoderTask();

  uint32_t ms = millis();
  if (ms - lastTelemMs >= TELEMETRY_PERIOD_MS) {
    lastTelemMs = ms;
    if (linkUp()) sendTelemetry();
  }
  if (ms - lastStatusMs >= STATUS_PERIOD_MS) {
    lastStatusMs = ms;
    mpuHz = mpuReads; mpuReads = 0;
    static uint32_t lastReport = 0, bootReported = 0;
    // Relatório no Serial: 3 s após o boot e a cada 10 s enquanto não houver link
    if ((!bootReported && ms > 3000) || (!linkUp() && ms - lastReport >= 10000)) {
      bootReported = 1; lastReport = ms; printSensorReport();
    }
    if (linkUp()) sendStatus();
    loopMaxUs = 0;

    uint32_t heap = ESP.getFreeHeap();
    if (heap < HEAP_CRITICAL_BYTES) {
      servoSafe("memória crítica");
      logEvent("err", "Memória livre crítica (%lu B) — reiniciando", (unsigned long)heap);
      delay(200);
      ESP.restart();
    } else if (heap < HEAP_WARN_BYTES) {
      logEvent("warn", "Memória livre baixa: %lu B", (unsigned long)heap);
    }
  }

  uint32_t dt = micros() - t0;
  if (dt > loopMaxUs) loopMaxUs = dt;
}
