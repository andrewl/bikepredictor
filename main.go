package main

import (
	"database/sql"
	"strconv"
	//"encoding/csv"
	"encoding/json"
	"fmt"

	"github.com/sjwhitworth/golearn/base"
	"github.com/sjwhitworth/golearn/evaluation"
	//"github.com/sjwhitworth/golearn/evaluation"
	"io/ioutil"
	"os"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/sjwhitworth/golearn/knn"
)

type Status struct {
	Name        string
	SchemeID    string
	DockId      string
	RequestTime string
	Bikes       int
	Docks       int
}

var db *sql.DB
var err error
var time_layout = "2006-01-02T15:04:05Z"

func main() {
	//cls := knn.NewKnnClassifier("euclidean", 2)
	json_file := os.Args[1]
	if json_file == "" {
		fmt.Printf("No filename passed: %v\n")
		os.Exit(1)
	}
	db, err = sql.Open("mysql", "root:H3nry2mysql@/bike_predictor")
	if err != nil {
		fmt.Printf("MySQL error: %v\n", err)
		os.Exit(1)
	}
	defer db.Close()
	//import_file(json_file)
	targetTime, _ := time.Parse("2006-01-02 15:04", "2017-01-05 09:00")
	predict("London", "102", targetTime)
}

func import_file(filename string) {

	file, err := ioutil.ReadFile(filename)
	if err != nil {
		fmt.Printf("File error: %v\n", err)
		os.Exit(1)
	}

	var statuses []Status
	json.Unmarshal(file, &statuses)

	tx, err := db.Begin()
	if err != nil {
		fmt.Printf("MySQL error: %v\n", err)
		os.Exit(1)
	}

	stmtIns, err := tx.Prepare("INSERT INTO statuses(SchemeId, DockId, Name, Bikes, Docks, cDay, cMonth, cMinuteOfDay) VALUES(?,?,?,?,?,?,?,?)")
	if err != nil {
		fmt.Printf("MySQL error: %v\n", err)
		os.Exit(1)
	}
	defer stmtIns.Close()

	for _, status := range statuses {
		t, err := time.Parse(time_layout, status.RequestTime)
		if err != nil {
			fmt.Printf("Date error: %v\n", err)
		}

		minute_of_day := t.Hour()*60 + t.Minute()

		_, err = stmtIns.Exec(status.SchemeID,
			status.DockId,
			status.Name,
			status.Bikes,
			status.Docks,
			t.Weekday(),
			t.Month(),
			minute_of_day,
		)
		if err != nil {
			fmt.Printf("Error inserting row: %v\n", err)
		}

	}

	err = tx.Commit()
	if err != nil {
		fmt.Printf("MySQL error: %v\n", err)
	}

}

func predict(scheme string, dockid string, targetTime time.Time) {

	// Let's create some attributes
	attrs := make([]base.Attribute, 4)
	attrs[0] = base.NewFloatAttribute("dayofweek")
	attrs[1] = base.NewFloatAttribute("month")
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
		fmt.Printf("Failed to scan for row count %v", err)
		os.Exit(1)
	}
	// Allocate space
	trainData.Extend(count)

	rows, err := db.Query("Select Bikes, Docks, cDay, cMinuteOfDay, cMonth from bike_predictor.statuses where SchemeID = ? and DockId = ?", "London", "102")
	if err != nil {
		fmt.Printf("%v", err)
		os.Exit(1)
	}
	defer rows.Close()
	var bikes int
	var docks int
	var cDay string
	var cMonth string
	var cMinuteOfDay string
	var status string
	var i int
	/*
		record := []string{"dayofweek", "month", "minuteofday", "label"}
		w.Write(record)
	*/
	i = 0

	for rows.Next() {

		if err := rows.Scan(&bikes, &docks, &cDay, &cMonth, &cMinuteOfDay); err != nil {
			fmt.Printf("%v", err)
			os.Exit(1)
		}

		if bikes > 3 {
			status = "bikes"
		}
		if docks > 3 {
			status = "docks"
		}
		if bikes > 3 && docks > 3 {
			status = "both"
		}
		if bikes < 3 && docks < 3 {
			status = "none"
		}

		/*
			record := []string{cDay, cMonth, cMinuteOfDay, status}

			w.Write(record)
		*/

		// Write the data
		trainData.Set(trainSpecs[0], i, trainSpecs[0].GetAttribute().GetSysValFromString(cDay))
		trainData.Set(trainSpecs[1], i, trainSpecs[1].GetAttribute().GetSysValFromString(cMonth))
		trainData.Set(trainSpecs[2], i, trainSpecs[2].GetAttribute().GetSysValFromString(cMinuteOfDay))
		trainData.Set(trainSpecs[3], i, trainSpecs[3].GetAttribute().GetSysValFromString(status))
		i++

	}
	//w.Flush()
	if err := rows.Err(); err != nil {
		fmt.Printf("%v", err)
		os.Exit(1)
	}

	fmt.Println("%v", trainData)

	// Load in a dataset, with headers. Header attributes will be stored.
	// Think of instances as a Data Frame structure in R or Pandas.
	// You can also create instances from scratch.
	/**
	rawData, err := base.ParseCSVToInstances("./_predict.csv", true)
	if err != nil {
		panic(err)
	}
	fmt.Println("%v", rawData)
	*/

	// Let's create some attributes
	/*
		trainData.AddClassAttribute(attrs[3]);
			attrs := make([]base.Attribute, 4)
			attrs[0] = base.NewFloatAttribute("dayofweek")
			attrs[1] = base.NewFloatAttribute("month")
			attrs[2] = base.NewFloatAttribute("minuteofday")
			attrs[3] = new(base.CategoricalAttribute)
			attrs[3].SetName("label")
	*/

	// Now let's create the final instances set
	newInst := base.NewDenseInstances()
	newInst.AddClassAttribute(attrs[3])

	// Add the attributes
	newSpecs := make([]base.AttributeSpec, len(attrs))
	for i, a := range attrs {
		newSpecs[i] = newInst.AddAttribute(a)
	}
	// By convention
	newInst.AddClassAttribute(attrs[len(attrs)-1])

	// Allocate space
	newInst.Extend(1)

	// Write the data
	newInst.Set(newSpecs[0], 0, newSpecs[0].GetAttribute().GetSysValFromString(strconv.Itoa(int(targetTime.Weekday()))))
	newInst.Set(newSpecs[1], 0, newSpecs[1].GetAttribute().GetSysValFromString(strconv.Itoa(int(targetTime.Month()))))
	newInst.Set(newSpecs[2], 0, newSpecs[2].GetAttribute().GetSysValFromString(strconv.Itoa(targetTime.Minute()+(targetTime.Hour()*60))))
	newInst.Set(newSpecs[3], 0, newSpecs[3].GetAttribute().GetSysValFromString(""))

	fmt.Println(newInst)

	// Print a pleasant summary of your data.
	//fmt.Println(rawData)

	//Initialises a new KNN classifier
	cls := knn.NewKnnClassifier("euclidean", 2)

	//Do a training-test split
	//trainData, testData := base.InstancesTrainTestSplit(rawData, 0.50)
	//fmt.Println(trainData)
	cls.Fit(trainData)

	//Calculates the Euclidean distance and returns the most popular label
	predictions, err := cls.Predict(newInst)
	if err != nil {
		panic(err)
	}

	fmt.Println("predictions - %v", predictions)

	// Prints precision/recall metrics
	confusionMat, err := evaluation.GetConfusionMatrix(newInst, predictions)
	if err != nil {
		panic(fmt.Sprintf("Unable to get confusion matrix: %s", err.Error()))
	}
	fmt.Println(evaluation.GetSummary(confusionMat))
	fmt.Printf("%v\n", targetTime)
	fmt.Printf("weekday %v\n", strconv.Itoa(int(targetTime.Weekday())))
	fmt.Printf("month %v\n", strconv.Itoa(int(targetTime.Month())))

}
