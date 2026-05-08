// Smart Vault Security Monitor
// Arduino Mega 2560
//
// Wiring:
//   A0    - Photoresistor (voltage divider with 10K to GND)
//   Pin 2 - Tilt switch (ball switch, active LOW = tilted)
//   Pin 3 - RGB LED Red   (220 ohm resistor)
//   Pin 5 - RGB LED Green (220 ohm resistor)
//   Pin 6 - RGB LED Blue  (330 ohm resistor)
//   Pin 8 - Active buzzer (+)
//
// LED colors:
//   Green        - NORMAL
//   Blue steady  - DARK_ALERT
//   Blue blink   - DARK_AND_TILT
//   Yellow steady- LIGHT_SPIKE
//   Yellow blink - SPIKE_AND_TILT
//   Orange       - TILT_DETECTED
//   Cyan pulse   - SENSOR_FAULT
//   Red blink    - SENSOR_FAULT_AND_TILT
//
// Sends one JSON line per second over Serial at 9600 baud.
// Python gateway reads this and adds a UTC timestamp.

#define LIGHT_PIN    A0
#define TILT_PIN      2
#define LED_RED       3
#define LED_GREEN     5
#define LED_BLUE      6
#define BUZZER_PIN    8

#define DEVICE_ID   "vault-node-01"
#define LOCATION    "server-room-a"

#define LIGHT_DARK_THRESHOLD    150
#define LIGHT_SPIKE_THRESHOLD   950
#define SAMPLE_INTERVAL_MS     1000

#define BRIGHT  200
#define OFF       0

#define FAST_BLINK_ON    150
#define FAST_BLINK_OFF   150
#define SLOW_PULSE_STEP    5
#define SLOW_PULSE_DELAY  10

#define STATUS_BUF  24

#define PAT_STEADY      0
#define PAT_FAST_BLINK  1
#define PAT_SLOW_PULSE  2

unsigned long lastSampleTime = 0;
unsigned long lastLoopStart  = 0;
unsigned long readingId      = 0;

void setup() {
  Serial.begin(9600);
  while (!Serial) { ; }

  pinMode(TILT_PIN,   INPUT);
  pinMode(LED_RED,    OUTPUT);
  pinMode(LED_GREEN,  OUTPUT);
  pinMode(LED_BLUE,   OUTPUT);
  pinMode(BUZZER_PIN, OUTPUT);

  setLED(OFF, OFF, OFF);
  digitalWrite(BUZZER_PIN, LOW);

  // Boot animation confirms all LED channels work
  setLED(BRIGHT, OFF,    OFF);    delay(200);
  setLED(OFF,    OFF,    OFF);    delay(100);
  setLED(OFF,    BRIGHT, OFF);    delay(200);
  setLED(OFF,    OFF,    OFF);    delay(100);
  setLED(OFF,    OFF,    BRIGHT); delay(200);
  setLED(OFF,    OFF,    OFF);    delay(100);
  setLED(BRIGHT, BRIGHT, BRIGHT); delay(300);
  setLED(OFF,    OFF,    OFF);    delay(150);
  setLED(OFF,    BRIGHT, OFF);

  Serial.println(F("{\"event\":\"BOOT\",\"device\":\"" DEVICE_ID "\",\"msg\":\"SmartVault online\"}"));
}

void loop() {
  unsigned long now = millis();

  if ((now - lastSampleTime) >= SAMPLE_INTERVAL_MS) {
    unsigned long loopMs = now - lastSampleTime;
    lastSampleTime = now;
    readingId++;

    // Read twice and average to reduce ADC noise
    delay(5);
    int r1 = analogRead(LIGHT_PIN);
    delay(5);
    int r2 = analogRead(LIGHT_PIN);
    int lightValue = (r1 + r2) / 2;

    // Ball switch is active-low
    int isTilted = (digitalRead(TILT_PIN) == LOW) ? 1 : 0;

    // 0=fault, 1=dark, 2=normal, 3=spike
    int lightState;
    if      (lightValue == 0)                    lightState = 0;
    else if (lightValue < LIGHT_DARK_THRESHOLD)  lightState = 1;
    else if (lightValue > LIGHT_SPIKE_THRESHOLD) lightState = 3;
    else                                         lightState = 2;

    char status[STATUS_BUF];
    int  rC, gC, bC, pattern, severity;

    if (lightState == 0 && isTilted == 0) {
      strcpy(status, "SENSOR_FAULT");
      rC=OFF; gC=BRIGHT; bC=BRIGHT;
      pattern=PAT_SLOW_PULSE; severity=1;

    } else if (lightState == 0 && isTilted == 1) {
      strcpy(status, "SENSOR_FAULT_AND_TILT");
      rC=BRIGHT; gC=OFF; bC=OFF;
      pattern=PAT_FAST_BLINK; severity=2;

    } else if (lightState == 1 && isTilted == 0) {
      strcpy(status, "DARK_ALERT");
      rC=OFF; gC=OFF; bC=BRIGHT;
      pattern=PAT_STEADY; severity=1;

    } else if (lightState == 1 && isTilted == 1) {
      strcpy(status, "DARK_AND_TILT");
      rC=OFF; gC=OFF; bC=BRIGHT;
      pattern=PAT_FAST_BLINK; severity=2;

    } else if (lightState == 2 && isTilted == 0) {
      strcpy(status, "NORMAL");
      rC=OFF; gC=BRIGHT; bC=OFF;
      pattern=PAT_STEADY; severity=0;

    } else if (lightState == 2 && isTilted == 1) {
      strcpy(status, "TILT_DETECTED");
      rC=BRIGHT; gC=80; bC=OFF;
      pattern=PAT_STEADY; severity=1;

    } else if (lightState == 3 && isTilted == 0) {
      strcpy(status, "LIGHT_SPIKE");
      rC=BRIGHT; gC=BRIGHT; bC=OFF;
      pattern=PAT_STEADY; severity=1;

    } else {
      strcpy(status, "SPIKE_AND_TILT");
      rC=BRIGHT; gC=BRIGHT; bC=OFF;
      pattern=PAT_FAST_BLINK; severity=2;
    }

    applyPattern(rC, gC, bC, pattern);

    if (severity == 0) {
      digitalWrite(BUZZER_PIN, LOW);
    } else if (severity == 1) {
      if (lightState != 0) {
        digitalWrite(BUZZER_PIN, HIGH); delay(80);
        digitalWrite(BUZZER_PIN, LOW);
      }
    } else {
      digitalWrite(BUZZER_PIN, HIGH); delay(100);
      digitalWrite(BUZZER_PIN, LOW);  delay(80);
      digitalWrite(BUZZER_PIN, HIGH); delay(100);
      digitalWrite(BUZZER_PIN, LOW);
    }

    Serial.print(F("{\"id\":"));        Serial.print(readingId);
    Serial.print(F(",\"device\":\""));  Serial.print(F(DEVICE_ID));
    Serial.print(F("\",\"location\":\"")); Serial.print(F(LOCATION));
    Serial.print(F("\",\"light\":"));   Serial.print(lightValue);
    Serial.print(F(",\"tilt\":"));      Serial.print(isTilted);
    Serial.print(F(",\"status\":\""));  Serial.print(status);
    Serial.print(F("\",\"sev\":"));     Serial.print(severity);
    Serial.print(F(",\"uptime\":"));    Serial.print(millis() / 1000UL);
    Serial.print(F(",\"loop_ms\":"));   Serial.print(loopMs);
    Serial.println(F("}"));
  }

  if (Serial.available() > 0) {
    String cmd = Serial.readStringUntil('\n');
    cmd.trim();

    if (cmd.equals("ALERT")) {
      for (int i = 0; i < 3; i++) {
        setLED(BRIGHT, OFF, OFF); delay(150);
        setLED(OFF,    OFF, OFF); delay(150);
      }
      setLED(BRIGHT, OFF, OFF);
      digitalWrite(BUZZER_PIN, HIGH); delay(100);
      digitalWrite(BUZZER_PIN, LOW);  delay(80);
      digitalWrite(BUZZER_PIN, HIGH); delay(100);
      digitalWrite(BUZZER_PIN, LOW);
      Serial.println(F("{\"event\":\"CMD_ACK\",\"cmd\":\"ALERT\"}"));

    } else if (cmd.equals("RESET")) {
      setLED(OFF, BRIGHT, OFF);
      digitalWrite(BUZZER_PIN, LOW);
      Serial.println(F("{\"event\":\"CMD_ACK\",\"cmd\":\"RESET\"}"));
    }
  }
}

void applyPattern(int r, int g, int b, int pattern) {
  if (pattern == PAT_STEADY) {
    setLED(r, g, b);

  } else if (pattern == PAT_FAST_BLINK) {
    for (int i = 0; i < 2; i++) {
      setLED(r, g, b);       delay(FAST_BLINK_ON);
      setLED(OFF, OFF, OFF); delay(FAST_BLINK_OFF);
    }
    setLED(r, g, b);

  } else if (pattern == PAT_SLOW_PULSE) {
    for (int v = 0; v <= BRIGHT; v += SLOW_PULSE_STEP) {
      setLED((r > 0 ? v : 0), (g > 0 ? v : 0), (b > 0 ? v : 0));
      delay(SLOW_PULSE_DELAY);
    }
    for (int v = BRIGHT; v >= 0; v -= SLOW_PULSE_STEP) {
      setLED((r > 0 ? v : 0), (g > 0 ? v : 0), (b > 0 ? v : 0));
      delay(SLOW_PULSE_DELAY);
    }
    setLED(r, g, b);
  }
}

void setLED(int r, int g, int b) {
  analogWrite(LED_RED,   constrain(r, 0, 255));
  analogWrite(LED_GREEN, constrain(g, 0, 255));
  analogWrite(LED_BLUE,  constrain(b, 0, 255));
}