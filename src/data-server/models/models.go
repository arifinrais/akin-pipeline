package models

import "go.mongodb.org/mongo-driver/bson/primitive"

//Create Struct
type Viz struct {
	ID  		primitive.ObjectID 
	Year		int
	National	*ClassChild
	Province	*Region
	City		*Region
	Island		*Region
	DevMain		*Region
	DevEcon		*Region
}

type ClassParent struct {
	ClassID	string
	Total	float32
	Classes	*ClassChild
}

type ClassChild struct {
	ClassID string
	Total	float32
}

type Region struct {
	RegionID	int
	Total		float32
	Classes		*ClassParent
}
