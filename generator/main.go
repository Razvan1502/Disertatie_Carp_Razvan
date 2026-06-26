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

type SmartHomeEvent struct {
	HouseID     string  `json:"house_id"`
	Temperature float64 `json:"temperature"`
	EnergyUsage float64 `json:"energy_usage"`
	Humidity    float64 `json:"humidity"`
	EventTime   int64   `json:"event_time"`
	ArrivalTime int64   `json:"arrival_time"`
}

var startTime   time.Time
var networkMode int // 0: Chaos, 1: Short cycles (6 min), 3: Long cycles (45 min)
var datasetMode int // 0: SmartHome, 1: Taxi+simulated delay, 2: Taxi real OOO (dropoff-sorted)

var goroutineSem = make(chan struct{}, 500)

// Stationary regime: mixed delay distribution to simulate network jitter.
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

// Short-cycle regime: 6 min period — 0-2 min Excellent, 2-4 min Congested, 4-6 min Recovery.
func getDelayAdaptive() time.Duration {
	phase := int(time.Since(startTime).Minutes()) % 6
	r := rand.Float64()
	if phase < 2 {
		return time.Duration(rand.Intn(150)+50) * time.Millisecond
	}
	if phase < 4 {
		if r < 0.50 {
			return time.Duration(rand.Intn(5000)+5000) * time.Millisecond
		}
		return time.Duration(rand.Intn(300)+100) * time.Millisecond
	}
	return time.Duration(rand.Intn(1500)+500) * time.Millisecond
}

// Long-cycle regime: 45 min period — 0-15 min Excellent, 15-30 min Congested, 30-45 min Recovery.
// Congestion is deliberately aggressive (75% of events at 8-18 s) to stress adaptive strategies.
func getDelayAdaptiveLong() time.Duration {
	phase := int(time.Since(startTime).Minutes()) % 45
	r := rand.Float64()
	if phase < 15 {
		return time.Duration(rand.Intn(150)+50) * time.Millisecond
	}
	if phase < 30 {
		if r < 0.75 {
			return time.Duration(rand.Intn(10000)+8000) * time.Millisecond
		}
		return time.Duration(rand.Intn(300)+100) * time.Millisecond
	}
	return time.Duration(rand.Intn(4000)+1000) * time.Millisecond
}

func getSimulatedDelay() time.Duration {
	switch networkMode {
	case 1:
		return getDelayAdaptive()
	case 3:
		return getDelayAdaptiveLong()
	default:
		return getDelayChaos()
	}
}

// Direct send without simulated delay — OOO arises from the dropoff-sorted CSV order.
func sendEventDirect(writer *kafka.Writer, event SmartHomeEvent) {
	event.ArrivalTime = time.Now().UnixMilli()
	payload, _ := json.Marshal(event)
	if err := writer.WriteMessages(context.Background(), kafka.Message{
		Key:   []byte(event.HouseID),
		Value: payload,
	}); err != nil {
		log.Printf("Eroare Kafka: %v", err)
	}
}

func currentPhaseName() string {
	if networkMode == 3 {
		phase := int(time.Since(startTime).Minutes()) % 45
		if phase < 15 {
			return "EXCELENT(L)"
		}
		if phase < 30 {
			return "CONGESTIE(L)"
		}
		return "RECUPERARE(L)"
	}
	phase := int(time.Since(startTime).Minutes()) % 6
	if phase < 2 {
		return "EXCELENT"
	}
	if phase < 4 {
		return "CONGESTIE"
	}
	return "RECUPERARE"
}

// Async send via goroutine; arrival_time is stamped after the sleep so that
// arrival_time − event_time equals the simulated network delay seen by Flink.
func sendEvent(writer *kafka.Writer, event SmartHomeEvent) {
	go func(e SmartHomeEvent) {
		goroutineSem <- struct{}{}
		defer func() { <-goroutineSem }()

		time.Sleep(getSimulatedDelay())

		e.ArrivalTime = time.Now().UnixMilli()
		payload, _ := json.Marshal(e)
		if err := writer.WriteMessages(context.Background(), kafka.Message{
			Key:   []byte(e.HouseID),
			Value: payload,
		}); err != nil {
			log.Printf("Eroare Kafka: %v", err)
		}
	}(event)
}

// SmartHome: one CSV row → 100 events (one per synthetic house), event_time = now.
func produceHomeCEvents(writer *kafka.Writer, record []string) {
	energy, err1 := strconv.ParseFloat(record[1], 64)
	temp, err2 := strconv.ParseFloat(record[19], 64)
	hum, err3 := strconv.ParseFloat(record[21], 64)
	if err1 != nil || err2 != nil || err3 != nil {
		return
	}
	for i := 0; i < 100; i++ {
		sendEvent(writer, SmartHomeEvent{
			HouseID:     fmt.Sprintf("HOUSE_%d", i),
			Temperature: temp + (rand.Float64()*1.0 - 0.5),
			EnergyUsage: energy + (rand.Float64() * 0.05),
			Humidity:    hum + (rand.Float64() * 0.02),
			EventTime:   time.Now().UnixMilli(),
		})
	}
}

// Taxi: one CSV row → 1 event, event_time = pickup_datetime, OOO = trip duration.
// Fields reused from the shared schema: temperature=trip_distance, energy_usage=trip_duration_s/60, humidity=passenger_count.
const (
	latMin, latMax = 40.4774, 40.9176
	lonMin, lonMax = -74.2591, -73.7004
	gridSize       = 10
)

func taxiGridCell(lat, lon float64) string {
	if lat < latMin || lat > latMax || lon < lonMin || lon > lonMax {
		return "CELL_OUT"
	}
	cellX := int((lon - lonMin) / (lonMax - lonMin) * gridSize)
	cellY := int((lat - latMin) / (latMax - latMin) * gridSize)
	return fmt.Sprintf("CELL_%d_%d", cellX, cellY)
}

func produceTaxiEventDirect(writer *kafka.Writer, record []string) {
	const layout = "2006-01-02 15:04:05"
	pickupTime, err := time.Parse(layout, record[5])
	if err != nil {
		return
	}
	dist, err1 := strconv.ParseFloat(record[9], 64)
	secs, err2 := strconv.ParseFloat(record[8], 64)
	pass, err3 := strconv.ParseFloat(record[7], 64)
	lon,  err4 := strconv.ParseFloat(record[10], 64)
	lat,  err5 := strconv.ParseFloat(record[11], 64)
	if err1 != nil || err2 != nil || err3 != nil || err4 != nil || err5 != nil {
		return
	}
	sendEventDirect(writer, SmartHomeEvent{
		HouseID:     taxiGridCell(lat, lon),
		Temperature: dist,
		EnergyUsage: secs / 60.0,
		Humidity:    pass,
		EventTime:   pickupTime.UnixMilli(),
	})
}

func produceTaxiEvent(writer *kafka.Writer, record []string) {
	const layout = "2006-01-02 15:04:05"
	pickupTime, err := time.Parse(layout, record[5])
	if err != nil {
		return
	}
	dist, err1 := strconv.ParseFloat(record[9], 64)
	secs, err2 := strconv.ParseFloat(record[8], 64)
	pass, err3 := strconv.ParseFloat(record[7], 64)
	lon,  err4 := strconv.ParseFloat(record[10], 64)
	lat,  err5 := strconv.ParseFloat(record[11], 64)
	if err1 != nil || err2 != nil || err3 != nil || err4 != nil || err5 != nil {
		return
	}
	sendEvent(writer, SmartHomeEvent{
		HouseID:     taxiGridCell(lat, lon),
		Temperature: dist,
		EnergyUsage: secs / 60.0,
		Humidity:    pass,
		EventTime:   pickupTime.UnixMilli(),
	})
}

func main() {
	fmt.Println("╔══════════════════════════════════════╗")
	fmt.Println("║    Generator IoT - Date Streaming    ║")
	fmt.Println("╚══════════════════════════════════════╝")

	fmt.Println("\nSelectează dataset-ul:")
	fmt.Println("  1 - Smart Home (HomeC.csv)")
	fmt.Println("  2 - DEBS 2015 NYC Taxi + delay simulat (sorted by pickup)")
	fmt.Println("  3 - DEBS 2015 NYC Taxi Real OOO (sorted by dropoff, fără delay artificial)")
	fmt.Print("Dataset (1/2/3): ")
	var dsInput string
	fmt.Scanln(&dsInput)
	switch dsInput {
	case "2":
		datasetMode = 1
		fmt.Println("→ Dataset: DEBS 2015 NYC Taxi (delay simulat)")
	case "3":
		datasetMode = 2
		networkMode = 2
		fmt.Println("→ Dataset: DEBS 2015 NYC Taxi Real OOO (dropoff-sorted, fără delay)")
	default:
		datasetMode = 0
		fmt.Println("→ Dataset: Smart Home (HomeC)")
	}

	if datasetMode < 2 {
		fmt.Println("\nSelectează scenariul de rețea:")
		fmt.Println("  1 - Haos Random (distribuție staționară)")
		fmt.Println("  2 - Faze Scurte (ciclu 6 min: 2min Excelent→2min Congestie→2min Recuperare)")
		fmt.Println("  3 - Faze Lungi  (ciclu 45 min: 15min Excelent→15min Congestie→15min Recuperare)")
		fmt.Print("Scenariu (1/2/3): ")
		var netInput string
		fmt.Scanln(&netInput)
		switch netInput {
		case "2":
			networkMode = 1
			fmt.Println("→ Scenariu: Faze Scurte (ciclu 6 min)")
		case "3":
			networkMode = 3
			fmt.Println("→ Scenariu: Faze Lungi (ciclu 45 min)")
		default:
			networkMode = 0
			fmt.Println("→ Scenariu: Haos Random")
		}
	}

	startTime = time.Now()

	var csvPath string
	switch datasetMode {
	case 0:
		csvPath = `C:\Users\carpr\Disertatie\disertatie-iot\data\HomeC.csv`
	case 1:
		csvPath = `C:\Users\carpr\Disertatie\disertatie-iot\data\trip_data\trip_data_1_sorted.csv`
	case 2:
		csvPath = `C:\Users\carpr\Disertatie\disertatie-iot\data\trip_data\trip_data_1_dropoff_sorted.csv`
	}

	file, err := os.Open(csvPath)
	if err != nil {
		log.Fatalf("Nu pot deschide: %s", csvPath)
	}
	defer file.Close()

	reader := csv.NewReader(file)
	reader.Read() // skip header

	writer := &kafka.Writer{
		Addr:     kafka.TCP("localhost:9092"),
		Topic:    "iot-sensor-data",
		Balancer: &kafka.LeastBytes{},
		Async:    true,
	}
	defer writer.Close()

	fmt.Printf("\nSimularea a pornit la %s | Citim: %s\n\n", startTime.Format("15:04:05"), csvPath)

	rowCount := 0
	for {
		record, err := reader.Read()
		if err == io.EOF {
			fmt.Println("EOF — reluăm de la început...")
			file.Seek(0, 0)
			reader = csv.NewReader(file)
			reader.Read()
			continue
		}
		if err != nil {
			log.Printf("Eroare citire: %v", err)
			continue
		}

		switch datasetMode {
		case 0:
			produceHomeCEvents(writer, record)
			time.Sleep(1 * time.Second)
		case 1:
			produceTaxiEvent(writer, record)
			time.Sleep(10 * time.Millisecond)
		case 2:
			produceTaxiEventDirect(writer, record)
			time.Sleep(1 * time.Millisecond)
		}

		rowCount++
		reportEvery := 500
		if datasetMode == 0 {
			reportEvery = 30
		}
		if rowCount%reportEvery == 0 {
			switch datasetMode {
			case 0:
				events := rowCount * 100
				if networkMode == 0 {
					fmt.Printf("[HAOS] Rânduri CSV: %d | Evenimente Kafka: %d\n", rowCount, events)
				} else {
					fmt.Printf("[%s] Rânduri CSV: %d | Evenimente Kafka: %d\n", currentPhaseName(), rowCount, events)
				}
			case 1:
				if networkMode == 0 {
					fmt.Printf("[HAOS] Evenimente taxi (delay simulat): %d\n", rowCount)
				} else {
					fmt.Printf("[%s] Evenimente taxi (delay simulat): %d\n", currentPhaseName(), rowCount)
				}
			case 2:
				fmt.Printf("[REAL OOO] Evenimente taxi trimise: %d\n", rowCount)
			}
		}
	}
}
