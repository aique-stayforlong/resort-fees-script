package main

import (
	"bufio"
	"bytes"
	"encoding/csv"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"time"
)

const (
	APIBaseURL     = "https://demandapi.booking.com/3.1"
	httpRetries    = 5
	resortFeeID    = 2
	nights         = 3
	adults         = 2
	rooms          = 1
	currency       = "EUR"
	bookerCountry  = "es"
	bookerPlatform = "desktop"
	propertyIDsCSV = "property_ids.csv"
	numWorkers     = 2
	maxReqPerSec   = 4
)

var (
	authToken      string
	affiliateID    string
	maxPropertyIDs int
)

type summary struct {
	success  []int64
	notFound []int64
	failure  []int64
}

type row struct {
	PropertyID  int64   `json:"property_id"`
	Currency    string  `json:"currency"`
	Mode        string  `json:"mode"`
	UnitAmount  float64 `json:"unit_amount"`
	TotalAmount float64 `json:"total_amount"`
	Percentage  float64 `json:"percentage"`
}

func (r row) getValue() float64 {
	if r.Mode == "percentage" {
		return r.Percentage
	}

	if r.Mode == "per_stay" {
		return r.TotalAmount
	}

	return r.UnitAmount
}

func (r row) getBreakout() ([]string, error) {
	switch r.Mode {
	case "per_person_per_night":
		return []string{"person", "night"}, nil
	case "per_person_per_stay":
		return []string{"person", "stay"}, nil
	case "per_person":
		return []string{"person"}, nil
	case "per_night":
		return []string{"night"}, nil
	case "per_stay":
		return []string{"stay"}, nil
	case "percentage":
		return []string{"percentage"}, nil
	default:
		return []string{}, fmt.Errorf("unknown mode: %s", r.Mode)
	}
}

func (e extraCharge) isResortFee() bool {
	return e.Charge == resortFeeID
}

func (e extraCharge) hasAmount() bool {
	return e.TotalAmount > 0 || e.UnitAmount > 0 || e.Percentage > 0
}

type stay struct {
	checkin  string
	checkout string
}

type extraCharge struct {
	Charge      int     `json:"charge"`
	Mode        string  `json:"mode"`
	Percentage  float64 `json:"percentage"`
	TotalAmount float64 `json:"total_amount"`
	UnitAmount  float64 `json:"unit_amount"`
}

type availabilityResponse struct {
	Data struct {
		ID       int64  `json:"id"`
		Currency string `json:"currency"`
		Products []struct {
			ID    string `json:"id"`
			Room  int64  `json:"room"`
			Price struct {
				Base         float64 `json:"base"`
				Book         float64 `json:"book"`
				Total        float64 `json:"total"`
				ExtraCharges struct {
					Included    []extraCharge `json:"included"`
					Excluded    []extraCharge `json:"excluded"`
					Conditional []extraCharge `json:"conditional"`
				} `json:"extra_charges"`
			} `json:"price"`
		} `json:"products"`
	} `json:"data"`
	Errors []struct {
		ID      string `json:"id"`
		Message string `json:"message"`
	} `json:"errors"`
}

func main() {
	loadEnv()

	maxPropertyIDsFlag := flag.Int("max-num-ids", 0, "limits the number of property IDs to process (0 = no limit)")
	flag.Parse()
	maxPropertyIDs = *maxPropertyIDsFlag

	ids := loadIDs()
	if len(ids) == 0 {
		fmt.Println("no property IDs found")
		return
	}
	ids = limitIDs(ids, maxPropertyIDs)

	stay := buildStay()

	client := &http.Client{Timeout: 60 * time.Second}
	summary := summary{}

	execFile := createExecutionFile()
	defer execFile.Close()

	nextPropertyIDs := make(chan int64)
	var wg sync.WaitGroup
	var printMu sync.Mutex
	var summaryMu sync.Mutex
	limiter := time.NewTicker(time.Duration(float64(time.Second) / maxReqPerSec))
	defer limiter.Stop()

	for w := 0; w < numWorkers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for id := range nextPropertyIDs {
				<-limiter.C
				resp, err := fetchAvailability(client, id, stay, adults, rooms, currency)

				if err != nil {
					summaryMu.Lock()
					summary.failure = append(summary.failure, id)
					summaryMu.Unlock()
					continue
				}

				summaryMu.Lock()
				rows := toRows(id, resp, &summary)
				summaryMu.Unlock()
				if len(rows) == 0 {
					continue
				}

				printMu.Lock()
				printRows(execFile, rows)
				printMu.Unlock()
			}
		}()
	}

	go func() {
		for _, id := range ids {
			nextPropertyIDs <- id
		}
		close(nextPropertyIDs)
	}()

	wg.Wait()
	printSummary(summary)
}

func printSummary(summary summary) {
	fmt.Print("\nsummary:\n--------\n")
	fmt.Printf("- success: %d\n", len(summary.success))
	fmt.Printf("- not found: %d\n", len(summary.notFound))
	fmt.Printf("- failure: %s\n", joinIDs(summary.failure))
}

func joinIDs(ids []int64) string {
	strs := make([]string, len(ids))
	for i, id := range ids {
		strs[i] = strconv.FormatInt(id, 10)
	}
	return strings.Join(strs, ",")
}

func loadEnv() {
	_, sourceFile, _, ok := runtime.Caller(0)
	if !ok {
		log.Fatal("error obtaining .env file path")
	}
	envPath := filepath.Join(filepath.Dir(sourceFile), ".env")

	file, err := os.Open(envPath)
	if err != nil {
		log.Fatalf("error opening %s: %v", envPath, err)
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		key, value, found := strings.Cut(line, "=")
		if !found {
			continue
		}
		key = strings.TrimSpace(key)
		value = strings.TrimSpace(value)

		if os.Getenv(key) == "" {
			_ = os.Setenv(key, value)
		}
	}
	if err := scanner.Err(); err != nil {
		log.Fatalf("error reading %s: %v", envPath, err)
	}

	authToken = os.Getenv("BOOKING_TOKEN")
	affiliateID = os.Getenv("BOOKING_AFFILIATE_ID")
	if authToken == "" {
		log.Fatalf("missing BOOKING_TOKEN in %s", envPath)
	}
	if affiliateID == "" {
		log.Fatalf("missing BOOKING_AFFILIATE_ID in %s", envPath)
	}
}

func loadIDs() []int64 {
	_, sourceFile, _, ok := runtime.Caller(0)
	if !ok {
		log.Fatal("error obtaining .csv file path")
	}
	csvPath := filepath.Join(filepath.Dir(sourceFile), "data", propertyIDsCSV)

	file, err := os.Open(csvPath)
	if err != nil {
		log.Fatalf("error opening %s: %v", csvPath, err)
	}
	defer file.Close()

	reader := csv.NewReader(file)
	reader.FieldsPerRecord = -1

	var ids []int64
	for {
		record, err := reader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			log.Printf("error reading %s: %v\n", csvPath, err)
			continue
		}
		if len(record) == 0 {
			continue
		}
		value := strings.TrimSpace(record[0])
		if value == "" {
			continue
		}
		id, err := strconv.ParseInt(value, 10, 64)
		if err != nil {
			log.Printf("invalid id %q in %s: %v\n", value, csvPath, err)
			continue
		}
		ids = append(ids, id)
	}
	return ids
}

func limitIDs(ids []int64, max int) []int64 {
	if max != 0 && len(ids) > max {
		fmt.Printf("limiting property ids from %d to %d\n", len(ids), max)
		return ids[:max]
	}
	return ids
}

func buildStay() stay {
	checkin := time.Now().AddDate(0, 0, 5).Format("2006-01-02")
	ci, err := time.Parse("2006-01-02", checkin)
	if err != nil {
		log.Fatalf("invalid date %q: %v", ci, err)
	}
	return stay{
		checkin:  checkin,
		checkout: ci.AddDate(0, 0, nights).Format("2006-01-02"),
	}
}

func fetchAvailability(client *http.Client, id int64, st stay, adults, rooms int, currency string) (*availabilityResponse, error) {
	body := map[string]any{
		"accommodation": id,
		"checkin":       st.checkin,
		"checkout":      st.checkout,
		"guests":        map[string]any{"number_of_adults": adults, "number_of_rooms": rooms},
		"booker":        map[string]any{"country": bookerCountry, "platform": bookerPlatform},
		"currency":      currency,
		"extras":        []string{"extra_charges"},
	}
	payload, err := json.Marshal(body)
	if err != nil {
		return nil, err
	}

	backoff := 2 * time.Second
	for attempt := 0; attempt < httpRetries; attempt++ {
		req, err := http.NewRequest(http.MethodPost, APIBaseURL+"/accommodations/availability", bytes.NewReader(payload))
		if err != nil {
			return nil, err
		}
		req.Header.Set("Authorization", "Bearer "+authToken)
		req.Header.Set("X-Affiliate-Id", affiliateID)
		req.Header.Set("Content-Type", "application/json")

		resp, err := client.Do(req)
		if err != nil {
			time.Sleep(backoff)
			backoff *= 2
			continue
		}
		raw, _ := io.ReadAll(resp.Body)
		_ = resp.Body.Close()

		switch {
		case resp.StatusCode == http.StatusOK:
			var parsed availabilityResponse
			if err := json.Unmarshal(raw, &parsed); err != nil {
				return nil, fmt.Errorf("json: %w", err)
			}
			return &parsed, nil
		case resp.StatusCode == http.StatusTooManyRequests || resp.StatusCode >= 500:
			wait := backoff
			if ra := resp.Header.Get("Retry-After"); ra != "" {
				if secs, err := strconv.Atoi(ra); err == nil {
					wait = time.Duration(secs) * time.Second
				}
			}
			time.Sleep(wait)
			backoff *= 2
		default:
			return nil, fmt.Errorf("http %d: %s", resp.StatusCode, strings.TrimSpace(string(raw)))
		}
	}
	return nil, fmt.Errorf("retries exceeded")
}

func toRows(propID int64, resp *availabilityResponse, summary *summary) []row {
	for _, p := range resp.Data.Products {
		for _, c := range p.Price.ExtraCharges.Included {
			if !(c.isResortFee() && c.hasAmount()) {
				continue
			}
			summary.success = append(summary.success, propID)
			return []row{{
				PropertyID:  propID,
				Currency:    resp.Data.Currency,
				Mode:        c.Mode,
				UnitAmount:  c.UnitAmount,
				TotalAmount: c.TotalAmount,
				Percentage:  c.Percentage,
			}}
		}
	}
	summary.notFound = append(summary.notFound, propID)
	return nil
}

type outputRow struct {
	IDProperty int64    `json:"id_property"`
	Currency   string   `json:"currency"`
	Value      float64  `json:"value"`
	Breakout   []string `json:"breakout"`
}

func printRows(w io.Writer, rows []row) {
	enc := json.NewEncoder(w)
	for _, r := range rows {
		breakout, err := r.getBreakout()
		if err != nil {
			log.Printf("error getting breakout: %v\n", err)
			continue
		}
		out := outputRow{
			IDProperty: r.PropertyID,
			Currency:   r.Currency,
			Value:      r.getValue(),
			Breakout:   breakout,
		}
		if err := enc.Encode(out); err != nil {
			log.Printf("error encoding row: %v\n", err)
		}
	}
}

func createExecutionFile() *os.File {
	_, sourceFile, _, ok := runtime.Caller(0)
	if !ok {
		log.Fatal("error obtaining executions dir path")
	}
	dir := filepath.Join(filepath.Dir(sourceFile), "executions")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		log.Fatalf("error creating %s: %v", dir, err)
	}

	name := time.Now().Format("2006-01-02_15-04-05") + ".jsonl"
	path := filepath.Join(dir, name)
	file, err := os.Create(path)
	if err != nil {
		log.Fatalf("error creating %s: %v", path, err)
	}
	return file
}
