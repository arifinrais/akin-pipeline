package main

import (
	"context"
	"encoding/json"
	"log"
	"net/http"

	"data-server/helper"
	"data-server/models"

	"github.com/gorilla/mux"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
)

//Connection mongoDB with helper class
var collection = helper.ConnectDB()

func getVisualization(w http.ResponseWriter, r *http.Request) {
	// set header
	w.Header().Set("Content-Type", "application/json")
	
	var visualization models.Visualization
	// we get params with mux.
	var params = mux.Vars(r)
	year, ipr_dim, reg_dim := params["year"], params["ipr_dimension"], params["reg_dimension"]

	// We create filter. If it is unnecessary to sort data for you, you can use bson.M{}
	filter := bson.M{"_id": id}
	err := collection.FindOne(context.TODO(), filter).Decode(&visualization)

	if err != nil {
		helper.GetError(err, w)
		return
	}

	json.NewEncoder(w).Encode(visualization)
}

func getAnalysis(w http.ResponseWriter, r *http.Request) {
	// set header.
	w.Header().Set("Content-Type", "application/json")
	//query: viz/anl ptn/trd/pub year province/city/.../kci/pci
	var analysis models.Analysis
	// we get params with mux.
	var params = mux.Vars(r)
	year, ipr_dim, reg_dim := params["year"], params["ipr_dimension"], params["reg_dimension"]

	// We create filter. If it is unnecessary to sort data for you, you can use bson.M{}
	filter := bson.M{"_id": id}
	err := collection.FindOne(context.TODO(), filter).Decode(&analysis)

	if err != nil {
		helper.GetError(err, w)
		return
	}

	json.NewEncoder(w).Encode(analysis)
}

// var client *mongo.Client

func main() {
	//Init Router
	r := mux.NewRouter()

	r.HandleFunc("/api/visualization", getVisualization).Methods("GET")
	r.HandleFunc("/api/analysis", getAnalysis).Methods("GET")

	config := helper.GetConfiguration()
	log.Fatal(http.ListenAndServe(config.Port, r))

}
