package main

import (
	elastic "gopkg.in/olivere/elastic.v3"

	"encoding/json"
	"fmt"
	"net/http"
	"reflect"
	"regexp"
	"time"

	"github.com/dgrijalva/jwt-go"
)

const (
	TYPE_USER = "user"
)

var (
	// username lower case + number + _
	// ^: the beginning
	// $: ending
	// [the message range]
	// _: one or more
	usernamePattern = regexp.MustCompile(`^[a-z0-9_]+$`).MatchString
)

type User struct {
	Username string `json:"username"`
	Password string `json:"password"`
	Age      int    `json:”age”`
	Gender   string `json:”gender”`
}

// checkUser checks whether user is valid
func checkUser(username, password string) bool {
	es_client, err := elastic.NewClient(elastic.SetURL(ES_URL), elastic.SetSniff(false))
	if err != nil {
		fmt.Printf("ES is not setup %v\n", err)
		return false
	}

	// index: name DB
	termQuery :=
		elastic.NewTermQuery("username", username)
	queryResult, err := es_client.Search().
		Index(INDEX).
		Query(termQuery).
		Pretty(true).
		Do()
	if err != nil {
		fmt.Printf("ES query failed %v\n", err)
		return false
	}

	var tyu User
	// return value is a slice
	// though iteration will run only once
	for _, item := range queryResult.Each(reflect.TypeOf(tyu)) {
		u := item.(User)
		return u.Password == password && u.Username == username
	}

	return false

}

// Add a user. return true if success
func addUser(user User) bool {
	es_client, err := elastic.NewClient(elastic.SetURL(ES_URL), elastic.SetSniff(false))
	if err != nil {
		fmt.Printf("ES is not setup %v\n", err)
		return false

	}

	// check if user exist
	termQuery := elastic.NewTermQuery("username", user.Username)
	queryResult, err := es_client.Search().
		Index(INDEX).
		Query(termQuery).
		Pretty(true).
		Do()
	if err != nil {
		fmt.Printf("ES query failed %v\n", err)
		return false
	}

	// just check if the user exist
	// just to see result is none
	if queryResult.TotalHits() > 0 {
		fmt.Printf("User %s already exists, cannot create a new user", user.Username)
		return false
	}

	_, err = es_client.Index().
		Index(INDEX).
		Type(TYPE_USER).
		Id(user.Username).
		BodyJson(user).
		Refresh(true).
		Do()
	if err != nil {
		fmt.Printf("ES save user failed")
		return false
	}
	return true
}

func signupHandler(w http.ResponseWriter, r *http.Request) {
	fmt.Println("Received one sign up")

	decoder := json.NewDecoder(r.Body)
	var u User
	if err := decoder.Decode(&u); err != nil {
		panic(err)
	}

	if u.Username != "" && u.Password != "" && usernamePattern(u.Username) {
		if addUser(u) {
			fmt.Println("User added successfully")
			w.Write([]byte("User added successfully"))
		} else {
			fmt.Println("Failed to add a new user.")
			http.Error(w, "Failed to add a new user", http.StatusInternalServerError)

		}

	} else {
		fmt.Println("Empty password or username.")
		http.Error(w, "Empty password or username", http.StatusInternalServerError)

	}
	w.Header().Set("Content-Type", "text/plain")
	w.Header().Set("Access-Control-Allow-Origin", "*")

}

// If login is successful, a new token is created.
func loginHandler(w http.ResponseWriter, r *http.Request) {
	fmt.Println("Received one login request")

	decoder := json.NewDecoder(r.Body)
	var u User
	if err := decoder.Decode(&u); err != nil {
		panic(err)
		return
	}

	if checkUser(u.Username, u.Password) {
		// generate token
		token := jwt.New(jwt.SigningMethodHS256)
		// payload
		claims := token.Claims.(jwt.MapClaims)
		/* Set token claims */
		claims["username"] = u.Username
		claims["exp"] = time.Now().Add(time.Hour * 24).Unix()

		/* Sign the token with our secret */
		tokenString, _ := token.SignedString(mySigningKey)

		/* Finally, write the token to the browser window */
		w.Write([]byte(tokenString))
	} else {
		fmt.Println("Invalid password or username.")
		http.Error(w, "Invalid password or username", http.StatusForbidden)
	}

	w.Header().Set("Content-Type", "text/plain")
	w.Header().Set("Access-Control-Allow-Origin", "*")
}
