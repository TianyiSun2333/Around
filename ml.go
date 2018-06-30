package main

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/pkg/errors"
	"golang.org/x/oauth2/google"
	"io"
	"io/ioutil"
	"net/http"
	"strings"
)

// Prediction struct to parse the result
// <Prediction> index of our model
type Prediction struct {
	// like index of the model
	Prediction int `json:"prediction"`
	// lable
	Key string `json:"key"`
	// the two score
	// <probability> <xxx>
	Scores []float64 `json:"scores"`
}

type MlResponse struct {
	Predictions []Prediction `json:"predictions"`
}

// parse input
// ImageBytes: the type of the file
type ImageBytes struct {
	B64 []byte `json:"b64"`
}
type Instance struct {
	// image byte-version content
	ImageBytes ImageBytes `json:"image_bytes"`
	// when multiple model to indicate
	// <tag> to represent that in google api
	Key string `json:"key"`
}

type MlRequest struct {
	Instances []Instance `json:"instances"`
}

var (
	// TODO: Replace this project name and model name with your configuration.
	project = "sigma-sunlight-206505"
	model   = "face"
	url     = "https://ml.googleapis.com/v1/projects/" + project + "/models/" + model + ":predict"
	scope   = "https://www.googleapis.com/auth/cloud-platform"
)

// <io.Reader>: this image
// return <float64>: the final score(probability)
// Annotate a image file based on ml model, return score and error if exists.
func annotate(r io.Reader) (float64, error) {
	ctx := context.Background()
	// read to byte array from image
	buf, _ := ioutil.ReadAll(r)

	ts, err := google.DefaultTokenSource(ctx, scope)
	if err != nil {
		fmt.Printf("failed to create token %v\n", err)
		return 0.0, err
	}
	tt, _ := ts.Token()

	// Construct a ml request.
	// MLRequest is in the memory, so use pointer
	request := &MlRequest{
		Instances: []Instance{
			{
				ImageBytes: ImageBytes{
					B64: buf,
				},
				Key: "1", // Does not matter to the client, it's for Google tracking.
			},
		},
	}
	// change this request to json
	body, _ := json.Marshal(request)

	// Construct a http request.
	req, _ := http.NewRequest("POST", url, strings.NewReader(string(body)))
	req.Header.Set("Authorization", "Bearer "+tt.AccessToken)

	fmt.Printf("Sending request to ml engine for prediction %s with token as %s\n", url, tt.AccessToken)

	// Send request to Google.
	client := &http.Client{}
	res, err := client.Do(req)
	if err != nil {
		fmt.Printf("failed to send ml request %v\n", err)
		return 0.0, err
	}
	var resp MlResponse
	body, _ = ioutil.ReadAll(res.Body)

	// Double check if the response is empty. Sometimes Google does not return an error instead just an
	// empty response while usually it's due to auth.
	if len(body) == 0 {
		fmt.Println("empty google response")
		return 0.0, errors.New("empty google response")
	}

	// Unmarshal:
	// transfer the response to our type
	if err := json.Unmarshal(body, &resp); err != nil {
		fmt.Printf("failed to parse response %v\n", err)
		return 0.0, err
	}

	if len(resp.Predictions) == 0 {
		// If the response is not empty, Google returns a different format. Check the raw message.
		// Sometimes it's due to the image format. Google only accepts jpeg don't send png or others.
		fmt.Printf("failed to parse response %s\n", string(body))
		return 0.0, errors.Errorf("cannot parse response %s\n", string(body))
	}
	// TODO: update index based on your ml model.
	results := resp.Predictions[0]
	fmt.Printf("Received a prediction result %f\n", results.Scores[0])
	// Score[0]: the probability that this graph is a face
	return results.Scores[0], nil
}
