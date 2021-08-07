package main

import (
	"context"
	"data-server/helper"
	"encoding/json"
	"log"
	"net/http"
	"strconv"

	"github.com/gorilla/mux"
	"go.mongodb.org/mongo-driver/bson"
)

//Connection mongoDB with helper class
var mongoClient = helper.ConnectDB()

func getVisualization(w http.ResponseWriter, r *http.Request) {
	// set header
	w.Header().Set("Content-Type", "application/json")

	var visualization *bson.M
	var params = r.URL.Query()

	year, ipr_dim := params["year"][0], params["ipr_dim"][0]
	yearInt, _ := strconv.Atoi(year)
	filter := bson.M{"year": yearInt}
	collection := mongoClient.Database("akin").Collection(helper.CollectionName(ipr_dim, "viz"))
	err := collection.FindOne(context.TODO(), filter).Decode(&visualization)

	defer func() {
		if err != nil {
			errorBson := bson.M{"errormsg": err.Error()}
			json.NewEncoder(w).Encode(errorBson)
		}
	}()
	json.NewEncoder(w).Encode(visualization)
}

func getAnalysis(w http.ResponseWriter, r *http.Request) {
	// set header.
	w.Header().Set("Content-Type", "application/json")

	var analysis *bson.M
	var params = r.URL.Query()

	year, ipr_dim := params["year"][0], params["ipr_dim"][0]
	yearInt, _ := strconv.Atoi(year)
	filter := bson.M{"year": yearInt}
	collection := mongoClient.Database("akin").Collection(helper.CollectionName(ipr_dim, "anl"))
	err := collection.FindOne(context.TODO(), filter).Decode(&analysis)

	defer func() {
		if err != nil {
			errorBson := bson.M{"errormsg": err.Error()}
			json.NewEncoder(w).Encode(errorBson)
		}
	}()
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
