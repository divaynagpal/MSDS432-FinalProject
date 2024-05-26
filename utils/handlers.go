package utils

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"net/http"
)

var (
	err error
)

type App struct {
	DB *sql.DB
}

var (
	queryReport1  = "SELECT * FROM RPTTBL01"
	queryReport2  = "SELECT * FROM RPTTBL02"
	queryReport3  = "SELECT * FROM RPTTBL03"
	queryReport4A = "SELECT * FROM RPTTBL04A"
	queryReport4B = "SELECT * FROM RPTTBL04B"
	queryReport4C = "SELECT * FROM RPTTBL04C"
	queryReport5  = "SELECT * FROM RPTTBL05"
	queryReport6  = "SELECT * FROM RPTTBL06"
	queryReport7  = "SELECT * FROM RPTTBL07"
)

func (app *App) GetReport1(w http.ResponseWriter, r *http.Request) {
	fmt.Println("Got Request")
	rows, err := app.DB.Query(queryReport1)
	if err != nil {
		http.Error(w, fmt.Sprintf("Query error: %v", err), http.StatusInternalServerError)
		return
	}
	results, shouldReturn := getDataFromTable(rows, w)
	if shouldReturn {
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(results)
}

func (app *App) GetReport2(w http.ResponseWriter, r *http.Request) {
	fmt.Println("Got Request")
	rows, err := app.DB.Query(queryReport2)
	if err != nil {
		http.Error(w, fmt.Sprintf("Query error: %v", err), http.StatusInternalServerError)
		return
	}

	results, shouldReturn := getDataFromTable(rows, w)
	if shouldReturn {
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(results)
}

func (app *App) GetReport3(w http.ResponseWriter, r *http.Request) {
	fmt.Println("Got Request")
	rows, err := app.DB.Query(queryReport3)
	if err != nil {
		http.Error(w, fmt.Sprintf("Query error: %v", err), http.StatusInternalServerError)
		return
	}

	results, shouldReturn := getDataFromTable(rows, w)
	if shouldReturn {
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(results)
}

func (app *App) GetReport4a(w http.ResponseWriter, r *http.Request) {
	fmt.Println("Got Request")
	rows, err := app.DB.Query(queryReport4A)
	if err != nil {
		http.Error(w, fmt.Sprintf("Query error: %v", err), http.StatusInternalServerError)
		return
	}

	results, shouldReturn := getDataFromTable(rows, w)
	if shouldReturn {
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(results)
}

func (app *App) GetReport4b(w http.ResponseWriter, r *http.Request) {
	fmt.Println("Got Request")
	rows, err := app.DB.Query(queryReport4B)
	if err != nil {
		http.Error(w, fmt.Sprintf("Query error: %v", err), http.StatusInternalServerError)
		return
	}

	results, shouldReturn := getDataFromTable(rows, w)
	if shouldReturn {
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(results)
}

func (app *App) GetReport4c(w http.ResponseWriter, r *http.Request) {
	fmt.Println("Got Request")
	rows, err := app.DB.Query(queryReport4C)
	if err != nil {
		http.Error(w, fmt.Sprintf("Query error: %v", err), http.StatusInternalServerError)
		return
	}

	results, shouldReturn := getDataFromTable(rows, w)
	if shouldReturn {
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(results)
}

func (app *App) GetReport5(w http.ResponseWriter, r *http.Request) {
	fmt.Println("Got Request")
	rows, err := app.DB.Query(queryReport5)
	if err != nil {
		http.Error(w, fmt.Sprintf("Query error: %v", err), http.StatusInternalServerError)
		return
	}

	results, shouldReturn := getDataFromTable(rows, w)
	if shouldReturn {
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(results)
}

func (app *App) GetReport6(w http.ResponseWriter, r *http.Request) {
	fmt.Println("Got Request")
	rows, err := app.DB.Query(queryReport6)
	if err != nil {
		http.Error(w, fmt.Sprintf("Query error: %v", err), http.StatusInternalServerError)
		return
	}

	results, shouldReturn := getDataFromTable(rows, w)
	if shouldReturn {
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(results)
}

func (app *App) GetReport7(w http.ResponseWriter, r *http.Request) {
	fmt.Println("Got Request")
	rows, err := app.DB.Query(queryReport7)
	if err != nil {
		http.Error(w, fmt.Sprintf("Query error: %v", err), http.StatusInternalServerError)
		return
	}

	results, shouldReturn := getDataFromTable(rows, w)
	if shouldReturn {
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(results)
}

func getDataFromTable(rows *sql.Rows, w http.ResponseWriter) ([]map[string]interface{}, bool) {
	defer rows.Close()

	columns, err := rows.Columns()
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return nil, true
	}

	values := make([]interface{}, len(columns))
	valuePtrs := make([]interface{}, len(columns))
	for i := range values {
		valuePtrs[i] = &values[i]
	}

	var results []map[string]interface{}

	for rows.Next() {
		if err := rows.Scan(valuePtrs...); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return nil, true
		}

		row := make(map[string]interface{})
		for i, col := range columns {
			var v interface{}
			val := values[i]
			switch val.(type) {
			case int64:
				v = val.(int64)
			case float64:
				v = val.(float64)
			case bool:
				v = val.(bool)
			case []byte:
				v = string(val.([]byte))
			case nil:
				v = nil
			default:
				v = val
			}

			fmt.Print("col", col)
			fmt.Print("val", v)
			row[col] = v
		}
		results = append(results, row)
	}
	if err := rows.Err(); err != nil {
		http.Error(w, fmt.Sprintf("Rows error: %v", err), http.StatusInternalServerError)
		return nil, true
	}
	return results, false
}
