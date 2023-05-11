package main

import (
	"database/sql"
	"encoding/json"
	"log"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"

	_ "github.com/lib/pq"
)

type Station struct {
	StationNumber int             `json:"station_number"`
	StationName   string          `json:"station_name"`
	Latitude      float64         `json:"latitude"`
	Longitude     float64         `json:"longitude"`
	Elevation     sql.NullFloat64 `json:"elevation"`
}

type Weather struct {
	ID            int             `json:"id"`
	DDDCar        int             `json:"ddd_car"`
	Tanggal       time.Time       `json:"tanggal"`
	StationNumber int             `json:"station_number"`
	Tn            sql.NullFloat64 `json:"tn"`
	Tx            sql.NullFloat64 `json:"tx"`
	Tavg          sql.NullFloat64 `json:"tavg"`
	RHavg         sql.NullFloat64 `json:"rh_avg"`
	RR            sql.NullFloat64 `json:"rr"`
	Ss            sql.NullFloat64 `json:"ss"`
	Ffx           sql.NullFloat64 `json:"ff_x"`
	DDDX          sql.NullInt64   `json:"ddd_x"`
	Ffavg         sql.NullFloat64 `json:"ff_avg"`
}

func (s Station) MarshalJSON() ([]byte, error) {
	type Alias Station // Create an alias of the Station struct to avoid infinite recursion
	if s.Elevation.Valid {
		// Return the elevation value if it's valid
		return json.Marshal(&struct {
			Alias
			Elevation float64 `json:"elevation"`
		}{
			Alias:     (Alias)(s),
			Elevation: s.Elevation.Float64,
		})
	} else {
		// Return null if the elevation is not valid
		return json.Marshal(&struct {
			Alias
			Elevation interface{} `json:"elevation"`
		}{
			Alias:     (Alias)(s),
			Elevation: nil,
		})
	}
}

func main() {
	// PostgreSQL connection details
	connStr := os.Getenv("PSQL")

	db, err := sql.Open("postgres", connStr)
	defer db.Close()
	if err != nil {
		log.Fatal(err)
	}
	// Execute the query to retrieve table names
	rows, err := db.Query("SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'")
	if err != nil {
		log.Fatal(err)
	}
	defer rows.Close()

	// Iterate over the rows and print the table names
	for rows.Next() {
		var tableName string
		err := rows.Scan(&tableName)
		if err != nil {
			log.Fatal(err)
		}
	}

	// Check for any errors during iteration
	err = rows.Err()
	if err != nil {
		log.Fatal(err)
	}

	// Define the route handler for fetching all rows
	http.HandleFunc("/stations", func(w http.ResponseWriter, r *http.Request) {

		// Enable CORS
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Access-Control-Allow-Methods", "GET")
		w.Header().Set("Access-Control-Allow-Headers", "Content-Type")

		// Execute the query
		rows, err := db.Query("SELECT * FROM \"Station\"")
		if err != nil {
			log.Fatal(err)
		}
		defer rows.Close()

		// Iterate over the rows and store them in a slice
		stations := []Station{}
		for rows.Next() {
			var station Station
			err := rows.Scan(&station.StationNumber, &station.StationName, &station.Latitude, &station.Longitude, &station.Elevation)
			if err != nil {
				log.Fatal(err)
			}
			stations = append(stations, station)
		}

		// Check for any errors during iteration
		err = rows.Err()
		if err != nil {
			log.Fatal(err)
		}

		// Convert the slice to JSON
		jsonData, err := json.Marshal(stations)
		if err != nil {
			log.Fatal(err)
		}

		// Set the Content-Type header and write the JSON response
		w.Header().Set("Content-Type", "application/json")
		w.Write(jsonData)
	})

	http.HandleFunc("/input/data", func(w http.ResponseWriter, r *http.Request) {
		// Enable CORS
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Access-Control-Allow-Methods", "GET")
		w.Header().Set("Access-Control-Allow-Headers", "Content-Type")

		// Handle preflight requests
		if r.Method == http.MethodOptions {
			return
		}

		// Get the query parameters from the URL
		values := r.URL.Query()
		stationNumber := values.Get("stationNumber")
		dateRange := values.Get("dateRange")
		dataTypes := strings.Split(values.Get("type"), ",")

		// Wrap each dataType with double quotes
		for i := range dataTypes {
			dataTypes[i] = `"` + dataTypes[i] + `"`
		}

		// Join the dataTypes with comma delimiter
		dataType := strings.Join(dataTypes, ",")

		// Handle the case when dataTypes is empty
		if dataType == "\"\"" {
			http.Error(w, "Invalid request. Missing data types.", http.StatusBadRequest)
			return
		}

		if _, err := strconv.Atoi(stationNumber); err != nil {
			http.Error(w, "Invalid request. Missing data types.", http.StatusBadRequest)
			return
		}

		// Split the date range into start and end dates
		dateRangeParts := strings.Split(dateRange, ",")

		startDate, err := time.Parse("2006-01-02", dateRangeParts[0])
		if err != nil {
			log.Fatal(err)
		}

		endDate, err := time.Parse("2006-01-02", dateRangeParts[1])
		if err != nil {
			log.Fatal(err)
		}

		// Construct the SQL query based on the query parameters
		query := "SELECT " + dataType + ",\"Tanggal\" FROM \"Weather\" WHERE station_number = $1 AND TO_DATE(\"Tanggal\", 'YYYY-MM-DD') BETWEEN $2 AND $3"

		// Execute the query
		rows, err := db.Query(query, stationNumber, startDate.Format("2006-01-02"), endDate.Format("2006-01-02"))
		if err != nil {
			log.Fatal(err)
		}
		defer rows.Close()

		// for each database row / record, a map with the column names and row values is added to the allMaps slice
		var results []map[string]interface{}
		columns, err := rows.Columns()

		for rows.Next() {
			values := make([]interface{}, len(columns))
			pointers := make([]interface{}, len(columns))
			for i := range values {
				pointers[i] = &values[i]
			}
			err := rows.Scan(pointers...)
			if err != nil {
				log.Fatal(err)
			}
			resultMap := make(map[string]interface{})
			for i, val := range values {
				resultMap[columns[i]] = val
			}
			results = append(results, resultMap)
		}

		// Convert the results to JSON
		jsonData, err := json.Marshal(results)
		if err != nil {
			log.Fatal(err)
		}

		// Set the Content-Type header and write the JSON response
		w.Header().Set("Content-Type", "application/json")
		w.Write(jsonData)
	})

	// Start the server
	log.Fatal(http.ListenAndServe(":8080", nil))
}
