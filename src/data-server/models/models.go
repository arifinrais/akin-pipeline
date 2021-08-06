package models

import "go.mongodb.org/mongo-driver/bson/primitive"

//Create Struct
type Visualization struct {
	ID  		primitive.ObjectID 
	Year		int
	National	[]ClassChild
	Province	[]Region
	City		[]Region
	Island		[]Region
	DevMain		[]Region
	DevEcon		[]Region
}

type Analysis struct {
	ID			primitive.ObjectID 
	Year		int
	Province	*Indexes
	City		*Indexes
	Island		*Indexes
	DevMain		*Indexes
	DevEcon		*Indexes
}

type Indexes struct {
	KCI		[]KCIndex
	IPCI	[]IPCIndex
}

type KCIndex struct {
	RegionID	int
	Value	float32
}

type IPCIndex struct {
	ClassID	string
	Value	float32
}

type Region struct {
	RegionID	int
	Total		float32
	Classes		[]ClassParent
}

type ClassParent struct {
	ClassID	string
	Total	float32
	Classes	[]ClassChild
}

type ClassChild struct {
	ClassID string
	Total	float32
}

