package main

import (
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/go-kit/kit/log"
	_ "github.com/go-sql-driver/mysql"
	"github.com/sjwhitworth/golearn/base"
	"github.com/sjwhitworth/golearn/evaluation"
	"github.com/sjwhitworth/golearn/knn"
	"io/ioutil"
	"net/http"
	"os"
	"strconv"
	"time"
)

type PredictionResponse struct {
	Result     string
	Prediction string
	CM         string
}

type Status struct {
	Name        string
	SchemeID    string
	DockId      string
	RequestTime string
	Bikes       int
	Docks       int
}

// DB Connection
var db *sql.DB

// Logger for logging
var logger log.Logger

func main() {

	// Initialise logging
	logger = log.NewLogfmtLogger(os.Stderr)
	logger = log.NewContext(logger).With("ts", log.DefaultTimestampUTC, "caller", log.DefaultCaller)
	logger.Log("msg", "Starting bikepredictor")

	// Open a connection to the database
	var err error
	db, err = sql.Open("mysql", os.Getenv("BP_DATABASE_URL"))
	if err != nil {
		logger.Log("msg", "Failed to open database")
		os.Exit(1)
	}
	defer db.Close()

	// Setup the http handlers
	http.HandleFunc("/import_file", import_file_handler)
	http.HandleFunc("/predict", predict_handler)
	bind := fmt.Sprintf("%s:%s", os.Getenv("BP_IP"), os.Getenv("BP_PORT"))
	err = http.ListenAndServe(bind, nil)
	if err != nil {
		logger.Log("msg", "Failed to listen", "error", err)
		panic(err)
	}
}

// Handler for import file requests. Decoupled from the function so that
// the function can be called via other means
func import_file_handler(w http.ResponseWriter, r *http.Request) {
	filename := r.URL.Query().Get("filename")
	status, err := import_file(filename)

	if err != nil {
		http.Error(w, "There was an error", 500)
		return
	}

	w.Header().Set("Server", "bikepredictor")
	w.Header().Set("Content-Type", "application/json")
	w.Write([]byte(status))
}

// Imports a file from the filesystem into the database
func import_file(filename string) (string, error) {

	logger.Log("msg", "Importing file", "filename", filename)

	var time_layout = "2006-01-02T15:04:05Z"

	file, err := ioutil.ReadFile(filename)
	if err != nil {
		logger.Log("err", err)
		return "", err
	}

	var statuses []Status
	json.Unmarshal(file, &statuses)

	if len(statuses) < 1 {
		err = errors.New("json contained no statuses")
		logger.Log("err", err)
		return "", err
	}

	tx, err := db.Begin()
	if err != nil {
		logger.Log("msg", "failed to create db transaction", "error", err)
		return "", err
	}

	stmtIns, err := tx.Prepare("INSERT INTO statuses(SchemeId, DockId, Name, Bikes, Docks, cMonth, cDay, cMinuteOfDay) VALUES(?,?,?,?,?,?,?,?)")
	if err != nil {
		tx.Rollback()
		logger.Log("err", err)
		return "", err
	}
	defer stmtIns.Close()
	for _, status := range statuses {
		t, err := time.Parse(time_layout, status.RequestTime)
		if err != nil {
			logger.Log("err", err)
		}

		minute_of_day := t.Hour()*60 + t.Minute()

		_, err = stmtIns.Exec(status.SchemeID,
			status.DockId,
			status.Name,
			status.Bikes,
			status.Docks,
			t.Month(),
			t.Weekday(),
			minute_of_day,
		)
		if err != nil {
			logger.Log("err", err)
		}

	}

	err = tx.Commit()
	if err != nil {
		logger.Log("err", err)
	}

	return "OK", nil
}

// HTTP handler for predict function
func predict_handler(w http.ResponseWriter, r *http.Request) {
	scheme := r.URL.Query().Get("scheme")
	dockid := r.URL.Query().Get("dockid")
	targetTime, _ := time.Parse("200601021504", r.URL.Query().Get("targettime"))
	response, err := predict(scheme, dockid, targetTime)

	if err != nil {
		http.Error(w, "There was an error", 500)
		return
	}

	ret, err := json.Marshal(response)
	if err != nil {
		http.Error(w, "There was an error converting response to json", 500)
		return
	}
	w.Header().Set("Server", "bikepredictor")
	w.Header().Set("Content-Type", "application/json")
	w.Write([]byte(ret))
}

// Function to predict
func predict(scheme string, dockid string, targetTime time.Time) (PredictionResponse, error) {

	logger.Log("msg", "predicting", "scheme", scheme, "dockid", dockid, "time", targetTime)

	var ret PredictionResponse

	// Let's create some attributes
	attrs := make([]base.Attribute, 4)
	attrs[0] = base.NewFloatAttribute("month")
	attrs[1] = base.NewFloatAttribute("dayofweek")
	attrs[2] = base.NewFloatAttribute("minuteofday")
	attrs[3] = new(base.CategoricalAttribute)
	attrs[3].SetName("label")

	// Now let's create the final instances set
	trainData := base.NewDenseInstances()

	// Add the attributes
	trainSpecs := make([]base.AttributeSpec, len(attrs))
	for i, a := range attrs {
		trainSpecs[i] = trainData.AddAttribute(a)
	}

	// By convention
	trainData.AddClassAttribute(attrs[len(attrs)-1])

	rowCount := db.QueryRow("Select count(*) c from bike_predictor.statuses where SchemeID = ? and DockId = ?", scheme, dockid)
	var count int
	if err := rowCount.Scan(&count); err != nil {
		logger.Log("err", err)
		return ret, err
	}
	// Allocate space
	trainData.Extend(count)

	rows, err := db.Query("Select Bikes, Docks, cMonth, cDay, cMinuteOfDay from bike_predictor.statuses where SchemeID = ? and DockId = ?", scheme, dockid)
	if err != nil {
		logger.Log("err", err)
		return ret, err
	}
	defer rows.Close()
	var bikes int
	var docks int
	var cDay string
	var cMonth string
	var cMinuteOfDay string
	var status string
	var i int
	i = 0

	for rows.Next() {

		if err := rows.Scan(&bikes, &docks, &cMonth, &cDay, &cMinuteOfDay); err != nil {
			logger.Log("err", err)
			return ret, err
		}

		status = "both"
		if bikes < 5 {
			status = "docks"
		}
		if docks < 5 {
			status = "bikes"
		}
		if bikes < 5 && docks < 5 {
			status = "none"
		}

		// Save the data in the trainData array
		trainData.Set(trainSpecs[0], i, trainSpecs[0].GetAttribute().GetSysValFromString(cMonth))
		trainData.Set(trainSpecs[1], i, trainSpecs[1].GetAttribute().GetSysValFromString(cDay))
		trainData.Set(trainSpecs[2], i, trainSpecs[2].GetAttribute().GetSysValFromString(cMinuteOfDay))
		trainData.Set(trainSpecs[3], i, trainSpecs[3].GetAttribute().GetSysValFromString(status))
		i++

	}
	if err := rows.Err(); err != nil {
		logger.Log("err", err)
		return ret, err
	}

	fmt.Println("%v", trainData)

	// Now let's create the final instances set
	targetInst := base.NewDenseInstances()
	targetInst.AddClassAttribute(attrs[3])

	// Add the attributes
	targetSpecs := make([]base.AttributeSpec, len(attrs))
	for i, a := range attrs {
		targetSpecs[i] = targetInst.AddAttribute(a)
	}
	// By convention
	targetInst.AddClassAttribute(attrs[len(attrs)-1])

	// Allocate space
	targetInst.Extend(1)

	// Write the data
	targetInst.Set(targetSpecs[0], 0, targetSpecs[0].GetAttribute().GetSysValFromString(strconv.Itoa(int(targetTime.Month()))))
	targetInst.Set(targetSpecs[1], 0, targetSpecs[1].GetAttribute().GetSysValFromString(strconv.Itoa(int(targetTime.Weekday()))))
	targetInst.Set(targetSpecs[2], 0, targetSpecs[2].GetAttribute().GetSysValFromString(strconv.Itoa(targetTime.Minute()+(targetTime.Hour()*60))))
	targetInst.Set(targetSpecs[3], 0, targetSpecs[3].GetAttribute().GetSysValFromString(""))

	//Initialises a new KNN classifier
	cls := knn.NewKnnClassifier("euclidean", 2)

	//Fit the training data
	cls.Fit(trainData)

	//Calculate the Euclidean distance and predict the likely label
	prediction, err := cls.Predict(targetInst)
	if err != nil {
		logger.Log("err", err)
		return ret, err
	}

	// Prints precision/recall metrics
	CM, _ := evaluation.GetConfusionMatrix(targetInst, prediction)
	if err != nil {
		logger.Log("err", err)
		return ret, err
	}

	// Store our results in the structure and return it
	ret.Result = prediction.RowString(0)
	ret.Prediction = fmt.Sprintf("%v", prediction)
	ret.CM = fmt.Sprintf("%v", CM)

	return ret, nil
}
