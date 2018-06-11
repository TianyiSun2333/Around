package main

import (
	"cloud.google.com/go/storage"
	"context"
	"encoding/json"
	"fmt"
	"github.com/pborman/uuid"
	elastic "gopkg.in/olivere/elastic.v3"
	"io"
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
	Url      string   `json:"url"`
	Location Location `json:"location"`
}

const (
	BUCKET_NAME = "post-images-206505"
	DISTANCE    = "200km"
	INDEX       = "around" // to tell elastic that the user is around, not jupiter, like the name of DB
	TYPE        = "post"
	ES_URL      = "http://35.226.154.28:9200/" // the actually elastic server in GCE
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
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Access-Control-Allow-Origin", "*")
	w.Header().Set("Access-Control-Allow-Headers", "Content-Type,Authorization")

	// 32 << 20 is the maxMemory param for ParseMultipartForm, equals to 32MB (1MB = 1024 * 1024 bytes = 2^20 bytes)
	// After you call ParseMultipartForm, the file will be saved in the server memory with maxMemory size.
	// If the file size is larger than maxMemory, the rest of the data will be saved in a system temporary file.
	r.ParseMultipartForm(32 << 20)

	// Parse form data
	fmt.Printf("Received one post request %s\n", r.FormValue("message"))
	lat, _ := strconv.ParseFloat(r.FormValue("lat"), 64)
	lon, _ := strconv.ParseFloat(r.FormValue("lon"), 64)
	// get the string data
	p := &Post{
		User:    "1111",
		Message: r.FormValue("message"),
		Location: Location{
			Lat: lat,
			Lon: lon,
		},
	}

	id := uuid.New()

	// get the image
	// <file> <header>
	// FormFile: read file data
	file, _, err := r.FormFile("image")
	if err != nil {
		http.Error(w, "GCS is not setup", http.StatusInternalServerError)
		fmt.Printf("GCS is not setup %v.\n", err)
		panic(err)
	}
	defer file.Close()

	// when save to GCS, need access
	ctx := context.Background()

	_, attrs, err := saveToGCS(ctx, file, BUCKET_NAME, id)
	if err != nil {
		http.Error(w, "GCS is not setup", http.StatusInternalServerError)
		fmt.Printf("GCS is not setup %v\n", err)
		panic(err)
	}

	// when stored in GCS, the return url is attrs, save it to p.Url
	p.Url = attrs.MediaLink

	// save user post to es
	saveToES(p, id)
}

func saveToGCS(ctx context.Context, r io.Reader, bucketName, name string) (*storage.ObjectHandle, *storage.ObjectAttrs, error) {
	// create a client, like a connection
	client, err := storage.NewClient(ctx)
	if err != nil {
		return nil, nil, err
	}

	// create a bucket handle with a target name
	bucket := client.Bucket(bucketName)

	// ckeck if this bucket can be use
	if _, err := bucket.Attrs(ctx); err != nil {
		return nil, nil, err
	}

	obj := bucket.Object(name)
	wc := obj.NewWriter(ctx)

	// write to GCS
	if _, err := io.Copy(wc, r); err != nil {
		return nil, nil, err
	}

	if err := wc.Close(); err != nil {
		return nil, nil, err
	}

	// offer read access to all users
	// access control lease
	if err := obj.ACL().Set(ctx, storage.AllUsers, storage.RoleReader); err != nil {
		return nil, nil, err
	}

	attrs, err := obj.Attrs(ctx)
	fmt.Printf("Post is saved to GCS: %s\n", attrs.MediaLink)

	return obj, attrs, err
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
		// right error processing
		// fmt.PrintF(w, "search input should be double value")
		// panic(err) or return (both can shutdown the go routine)
		// panic(err) can print the stack trace in the console
		// return will not

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
