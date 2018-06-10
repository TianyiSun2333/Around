package main

import (
	"encoding/json"
	"fmt"
	"github.com/pborman/uuid"
	elastic "gopkg.in/olivere/elastic.v3"
	"log"
	"net/http"
	"reflect"
	"strconv"
)

// GO struct and JSON transfer

type Location struct {
	// float64: java double
	// java class
	// ``: tell Decode() that the name lat in JSON is represent Lat here
	// raw string can automatically make this change
	Lat float64 `json:"lat"` // ``: raw string: no escape character
	Lon float64 `json:"lon"`
}

// post behavior of user
type Post struct {
	// exported name must be capital
	User     string   `json:"user"`
	Message  string   `json:"message"`
	Location Location `json:"location"`
}

const (
	DISTANCE = "200km"
	INDEX    = "around" // to tell elastic that the user is around, not jupiter, like the name of DB
	TYPE     = "post"
	ES_URL   = "http://35.225.190.221:9200/" // the actually elastic server in GCE
)

func main() {

	// map location to geopoint

	// Create a client
	client, err := elastic.NewClient(elastic.SetURL(ES_URL), elastic.SetSniff(false))
	if err != nil {
		panic(err)
		return
	}

	// check if the connections is right, check also need a client
	// only need to create instance once
	// Use the IndexExists service to check if a specified index exists.
	exists, err := client.IndexExists(INDEX).Do()
	if err != nil {
		panic(err)
	}
	if !exists {
		// make location to a geopoint
		// Create a new index.
		mapping := `{
			"mappings":{
				"post":{
					"properties":{
						"location":{
							"type":"geo_point"
						}
					}
				}
			}
		}`
		_, err := client.CreateIndex(INDEX).Body(mapping).Do()
		if err != nil {
			// Handle error
			panic(err)
		}
	}

	fmt.Println("Started-service")
	// <endpoint> <which function endpoint are using>
	// like <servlet> <doPost>
	// handler is call back funtion, so there are concurrent
	// handlerPost and handlerSearch has two GO routine to manage them
	http.HandleFunc("/post", handlerPost)
	http.HandleFunc("/search", handlerSearch)
	// once error happens
	// <port> <handler>
	// handler has been create in last line
	// bound the handler to the port
	// wait for request, and call the callback function, once request coming,
	// create a go routine to call handler
	log.Fatal(http.ListenAndServe(":8080", nil))
}

// to handle post request
// GO pass by value
// therefore using pointer to simulate reference in java

// user pass a JSON to this, format must the same
// {
//   "user": "Tianyi",
//   "message": "666",
//   "locaiton": {
//      "lat": 37,
//      "lon": 40
//    }
//
// }
// JSON: snake case; to uniform the name writing between JSON and GO
func handlerPost(w http.ResponseWriter, r *http.Request) {
	fmt.Println("Received one post request.")

	// the body in the request
	// JSON from user
	decoder := json.NewDecoder(r.Body)
	var p Post

	// create new Post
	// get the data from user
	// ; two statements in the line
	// &p: Decode(<takes a pointer, so, need to pass an address>)
	// to change the value of p
	// Decode() only return a error
	// change JSON to our struct
	// once leave if, err can not be reached, life cycle like java
	if err := decoder.Decode(&p); err != nil {
		// if error happens
		// java throws
		panic(err)
	}
	// write in to w
	// file print format, anyone supporting IOstream can use this
	fmt.Fprintf(w, "Post received: %s\n", p.Message)

	// es use id to distince message
	// use uuid to generate a new id
	id := uuid.New()
	// save user post to es
	saveToES(&p, id)
}

// elastic search also stores data, is a DB
func saveToES(p *Post, id string) {
	es_client, err := elastic.NewClient(elastic.SetURL(ES_URL), elastic.SetSniff(false))
	if err != nil {
		panic(err)
	}

	_, err = es_client.Index().
		Index(INDEX).
		Type(TYPE).
		Id(id).
		BodyJson(p).
		Refresh(true).
		Do()

	if err != nil {
		panic(err)
	}

	fmt.Printf("Post is saved to index: %s\n", p.Message)
}

// get parameter from url
func handlerSearch(w http.ResponseWriter, r *http.Request) {
	fmt.Println("Received one request for search.")

	// <target string> <length of float>
	// _: I dont care about the value of return, (err)
	// in GO, cannot just initialize a varaible and not use it
	lat, _ := strconv.ParseFloat(r.URL.Query().Get("lat"), 64)
	lon, _ := strconv.ParseFloat(r.URL.Query().Get("lon"), 64)

	ran := DISTANCE
	if val := r.URL.Query().Get("range"); val != "" {
		ran = val + "km"
	}

	fmt.Printf("Search received: %f %f %s\n", lat, lon, ran)

	// client handle: like ticket master API
	// sniff: log (book-keeping by callback)
	client, err := elastic.NewClient(elastic.SetURL(ES_URL), elastic.SetSniff(false))
	if err != nil {
		panic(err)
	}

	// location: name of query
	// Define geo distance query as specified in
	// https://www.elastic.co/guide/en/elasticsearch/reference/5.2/query-dsl-geo-distance-query.html
	q := elastic.NewGeoDistanceQuery("location")
	q = q.Distance(ran).Lat(lat).Lon(lon)

	// interface(object)
	searchResult, err := client.Search().
		Index(INDEX).
		Query(q).
		Pretty(true).
		Do()

	if err != nil {
		panic(err)
	}

	fmt.Println("Query took %d milliseconds\n", searchResult.TookInMillis)
	fmt.Printf("Found a total of %d posts\n", searchResult.TotalHits())

	// put the result in Post
	var typ Post
	var ps []Post

	// elastic not know what type of element in the result, tell him it's Post
	// reflection: instance of Post
	// only pick Post type
	for _, item := range searchResult.Each(reflect.TypeOf(typ)) {
		p := item.(Post) // p = (Post) item
		fmt.Printf("Post by %s: %s at lat %v and lon %v\n",
			p.User, p.Message, p.Location.Lat, p.Location.Lon)
		ps = append(ps, p)
	}

	js, err := json.Marshal(ps)
	if err != nil {
		panic(err)
	}

	w.Header().Set("Content-Type", "application/json")
	// allow front end to have access
	w.Header().Set("Access-Control-Allow-Origin", "*")
	w.Write(js)
	// Return a fake post
	// convenient to transfer to JSON
	/*	p := &Post{
			User:    "1111",
			Message: "一生必去的100个地方",
			Location: Location{
				Lat: lat,
				Lon: lon,
			},
		}

		// to a JSON string, like java toString()
		js, err := json.Marshal(p)
		if err != nil {
			panic(err)
		}

		// tell browser that the return type of data
		w.Header().Set("Content-Type", "application/json")
		w.Write(js)
		fmt.Fprintf(w, "Search received: %s %s", lat, lon)
	*/
}
