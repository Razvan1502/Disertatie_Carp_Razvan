package main

import (
	"context"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"math/rand"
	"os"
	"strconv"
	"time"

	"github.com/segmentio/kafka-go"
)

// datele procesate de Flink
type SmartHomeEvent struct {
	HouseID     string  `json:"house_id"`
	Temperature float64 `json:"temperature"`
	EnergyUsage float64 `json:"energy_usage"`
	Humidity    float64 `json:"humidity"`
	EventTime   int64   `json:"event_time"`
	ArrivalTime int64   `json:"arrival_time"`
}

var startTime time.Time
var currentMode int // 0: Random Chaos, 1: Adaptive Phases

// Strategia 1:
func getDelayChaos() time.Duration {
	r := rand.Float64()
	if r < 0.15 {
		return time.Duration(rand.Intn(7)+5) * time.Second
	}
	if r < 0.30 {
		return time.Duration(rand.Intn(2000)) * time.Millisecond
	}
	return time.Duration(rand.Intn(200)+50) * time.Millisecond
}

// Strategia 2: Fazele Rețelei (Adaptive)
func getDelayAdaptive() time.Duration {
	elapsed := time.Since(startTime).Minutes()
	r := rand.Float64()

	if elapsed < 2 { // Excelent
		return time.Duration(rand.Intn(200)+50) * time.Millisecond
	}
	if elapsed >= 2 && elapsed < 4 { // Congestie
		if r < 0.80 {
			return time.Duration(rand.Intn(8)+6) * time.Second
		}
		return time.Duration(rand.Intn(500)+100) * time.Millisecond
	}
	return time.Duration(rand.Intn(1500)+500) * time.Millisecond // Recuperare
}

// Funcție wrapper
func getSimulatedDelay() time.Duration {
	if currentMode == 1 {
		return getDelayAdaptive()
	}
	return getDelayChaos()
}

func produceMultiplexedEvents(writer *kafka.Writer, record []string, currentTime time.Time) {

	energy, err1 := strconv.ParseFloat(record[1], 64)
	temp, err2 := strconv.ParseFloat(record[19], 64)
	hum, err3 := strconv.ParseFloat(record[21], 64)

	if err1 != nil || err2 != nil || err3 != nil {
		return
	}

	numHouses := 100

	for i := 0; i < numHouses; i++ {
		houseID := fmt.Sprintf("HOUSE_%d", i)

		event := SmartHomeEvent{
			HouseID:     houseID,
			Temperature: temp + (rand.Float64()*1.0 - 0.5),
			EnergyUsage: energy + (rand.Float64() * 0.05),
			Humidity:    hum + (rand.Float64() * 0.02),
			EventTime:   time.Now().UnixMilli(),
		}

		// Trimitem asincron cu delay pentru a genera Out-of-Order
		go func(e SmartHomeEvent) {
			delay := getSimulatedDelay()
			time.Sleep(delay)

			e.ArrivalTime = time.Now().UnixMilli()
			payload, _ := json.Marshal(e)

			err := writer.WriteMessages(context.Background(), kafka.Message{
				Key:   []byte(e.HouseID),
				Value: payload,
			})
			if err != nil {
				log.Printf("Eroare Kafka: %v", err)
			}
		}(event)
	}
}

func main() {

	fmt.Println("Selectează scenariul de simulare a rețelei:")
	fmt.Println("1 - Haos Random (Stabil)")
	fmt.Println("2 - Fazele Rețelei (Dinamic/Adaptive)")
	fmt.Print("Opțiunea ta (1/2): ")

	var input string
	fmt.Scanln(&input)
	if input == "2" {
		currentMode = 1
		fmt.Println("🚀 Ai ales scenariul: Fazele Rețelei")
	} else {
		currentMode = 0
		fmt.Println("🚀 Ai ales scenariul: Haos Random")
	}
	// Sincronizăm startTime pentru cazul 2
	startTime = time.Now()

	fmt.Printf("🚀 Simularea a început la: %s\n", startTime.Format("15:04:05"))

	csvPath := `C:\Users\carpr\Disertatie\disertatie-iot\data\HomeC.csv`

	file, err := os.Open(csvPath)
	if err != nil {
		log.Fatalf("Eroare: Nu pot deschide fișierul la calea %s. Verifică dacă folderul 'data' există.", csvPath)
	}
	defer file.Close()

	reader := csv.NewReader(file)
	// Citim header-ul și îl ignorăm
	_, _ = reader.Read()

	// Configurare Kafka
	writer := &kafka.Writer{
		Addr:     kafka.TCP("localhost:9092"),
		Topic:    "iot-sensor-data",
		Balancer: &kafka.LeastBytes{},
		Async:    true, // Permite trimiterea rapidă a mesajelor multiplexate
	}
	defer writer.Close()

	fmt.Printf("🚀 Orașul inteligent a pornit! Citim din: %s\n", csvPath)
	fmt.Println("Generăm 100 de evenimente (case) pentru fiecare secundă din CSV...")

	for {
		record, err := reader.Read()
		if err == io.EOF {
			fmt.Println("S-a terminat fișierul. O luăm de la capăt...")
			file.Seek(0, 0)
			reader.Read()
			continue
		}
		if err != nil {
			log.Printf("Eroare citire rând: %v", err)
			continue
		}

		// Trimitem lotul de case pentru rândul curent
		produceMultiplexedEvents(writer, record, time.Now())

		// Simulăm fluxul în timp real: un rând din CSV la fiecare secundă reală
		time.Sleep(1 * time.Second)

		if time.Now().Second()%10 == 0 {
			fmt.Printf("Status: Trimitere date în curs... (Time in CSV: %s)\n", record[0])
		}
	}
}
