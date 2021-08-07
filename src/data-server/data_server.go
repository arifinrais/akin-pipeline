package main

import (
	"context"
	"data-server/helper"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"strconv"

	"github.com/gorilla/mux"
	"go.mongodb.org/mongo-driver/bson"
)

var mongoClient = helper.ConnectDB()

func homePage(w http.ResponseWriter, r *http.Request) {
	fmt.Fprintf(w, "<h1>%s</h1><div>%s</div>", "AKIN-API", "Data-Server v1.0.0")
}

func getVisualization(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")

	var visualization *bson.M
	var params = r.URL.Query()

	year, ipr_dim := params["year"][0], params["ipr_dim"][0]
	yearInt, _ := strconv.Atoi(year)
	filter := bson.M{"year": yearInt}
	collection := mongoClient.Database("akin").Collection(helper.CollectionName(ipr_dim, "viz"))
	err := collection.FindOne(context.TODO(), filter).Decode(&visualization)

	if err != nil {
		helper.GetError(err, w)
		return
	}
	json.NewEncoder(w).Encode(visualization)
}

func getAnalysis(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")

	var analysis *bson.M
	var params = r.URL.Query()

	year, ipr_dim := params["year"][0], params["ipr_dim"][0]
	yearInt, _ := strconv.Atoi(year)
	filter := bson.M{"year": yearInt}
	collection := mongoClient.Database("akin").Collection(helper.CollectionName(ipr_dim, "anl"))
	err := collection.FindOne(context.TODO(), filter).Decode(&analysis)

	if err != nil {
		helper.GetError(err, w)
		return
	}
	json.NewEncoder(w).Encode(analysis)
}

func main() {
	r := mux.NewRouter()

	r.HandleFunc("/", homePage).Methods("GET")
	r.HandleFunc("/api/visualization", getVisualization).Methods("GET")
	r.HandleFunc("/api/analysis", getAnalysis).Methods("GET")

	config := helper.GetConfiguration()
	log.Fatal(http.ListenAndServe(config.Port, r))

}
