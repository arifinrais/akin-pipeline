package main

import (
	"context"
	"encoding/json"
	"log"
	"net/http"
	"fmt"
	"data-server/helper"
	"data-server/models"

	"github.com/gorilla/mux"
	"go.mongodb.org/mongo-driver/bson"
)

//Connection mongoDB with helper class
var mongoClient = helper.ConnectDB()

func getVisualization(w http.ResponseWriter, r *http.Request) {
	// set header
	w.Header().Set("Content-Type", "application/json")

	var visualization *bson.M
	// we get params with mux.
	var params = r.URL.Query()

	fmt.Println(params["year"])
	year, ipr_dim, reg_dim := params["year"][0], params["ipr_dim"][0], params["reg_dim"][0]

	// We create filter. If it is unnecessary to sort data for you, you can use bson.M{}
	filter := bson.M{"year": year}
	collection := mongoClient.Database("akin").Collection(helper.CollectionName(ipr_dim,"viz"))
	err := collection.FindOne(context.TODO(), filter).Decode(&visualization)

	if err != nil {
		helper.GetError(err, w)
		return
	}
	if reg_dim=="province" {
		json.NewEncoder(w).Encode(visualization)
	}
}

func getAnalysis(w http.ResponseWriter, r *http.Request) {
	// set header.
	w.Header().Set("Content-Type", "application/json")
	//query: viz/anl ptn/trd/pub year province/city/.../kci/pci
	var analysis models.Analysis
	// we get params with mux.
	var params = r.URL.Query()
	fmt.Println(params["year"])
	year, ipr_dim, reg_dim := params["year"][0], params["ipr_dim"][0], params["reg_dim"][0]

	// We create filter. If it is unnecessary to sort data for you, you can use bson.M{}
	filter := bson.M{"year": year}
	collection := mongoClient.Database("akin").Collection(helper.CollectionName(ipr_dim,"anl"))
	err := collection.FindOne(context.TODO(), filter).Decode(&analysis)

	if err != nil {
		helper.GetError(err, w)
		return
	}
	if reg_dim=="province" {
		json.NewEncoder(w).Encode(analysis.Province)
	}
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
