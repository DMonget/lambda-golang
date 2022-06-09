package main

import (
	"encoding/json"
	"errors"
	"io/ioutil"
	"log"
	"net/http"

	"github.com/aws/aws-lambda-go/events"
	"github.com/aws/aws-lambda-go/lambda"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/gocarina/gocsv"
)

var (
	// DefaultHTTPGetAddress Default Address
	DefaultHTTPGetAddress = "https://checkip.amazonaws.com"

	// ErrNoIP No IP found in response
	ErrNoIP = errors.New("No IP in HTTP response")

	// ErrNon200Response non 200 status code in response
	ErrNon200Response = errors.New("Non 200 Response found")
)

type App struct {
	S3 *s3.S3
}

type CsvEntry struct {
	ColA string `csv:"ColA"`
	ColB string `csv:"ColB"`
	ColC string `csv:"ColC"`
	ColD string `csv:"ColD"`
	ColE string `csv:"ColE"`
	ColF string `csv:"ColF"`
	ColG string `csv:"ColG"`
	ColH string `csv:"ColH"`
	ColI string `csv:"ColI"`
	ColJ string `csv:"ColJ"`
}

type FileParseResult struct {
	FileName string
	RowCount int
}

func (app App) getBucketContent(bucketName string) *s3.ListObjectsOutput {
	req, err := app.S3.ListObjects(&s3.ListObjectsInput{
		Bucket: aws.String(bucketName),
	})

	if err != nil {
		log.Fatalf("Unable list objects in %q, %v", bucketName, err)
	}

	return req
}

func (app App) getFileFromBucket(bucketName, fileName string) *s3.GetObjectOutput {
	rawData, err := app.S3.GetObject(
		&s3.GetObjectInput{
			Bucket: aws.String(bucketName),
			Key:    aws.String(fileName),
		})

	if err != nil {
		log.Fatalf("Unable to get object %q, %v", fileName, err)
	}

	return rawData
}

func (app App) parseAndCount(bucketName string) []FileParseResult {
	// List the csv files in the bucket
	req := app.getBucketContent(bucketName)

	var fileParseResults []FileParseResult
	// For each file in the bucket :
	for _, file := range req.Contents {
		// Retrieve the current file from the bucket
		rawData := app.getFileFromBucket(bucketName, *file.Key)

		//Read the file content as CSV
		bytes, err := ioutil.ReadAll(rawData.Body)
		if err != nil {
			log.Fatal(err)
		}

		var entryList []CsvEntry
		gocsv.UnmarshalBytes(bytes, entryList)

		// Add parse the csv read result, and add the data to the parse results
		fileParseResults = append(fileParseResults,
			FileParseResult{FileName: *file.Key, RowCount: len(entryList)})
	}
	return fileParseResults
}

func (app App) handler(request events.APIGatewayProxyRequest) (events.APIGatewayProxyResponse, error) {
	resp, err := http.Get(DefaultHTTPGetAddress)
	if err != nil {
		return events.APIGatewayProxyResponse{}, err
	}

	if resp.StatusCode != 200 {
		return events.APIGatewayProxyResponse{}, ErrNon200Response
	}

	ip, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return events.APIGatewayProxyResponse{}, err
	}

	if len(ip) == 0 {
		return events.APIGatewayProxyResponse{}, ErrNoIP
	}

	parseResults := app.parseAndCount("testperf-bucket")
	out, err := json.Marshal(parseResults)
	if err != nil {
		panic(err)
	}

	return events.APIGatewayProxyResponse{
		Body:       string(out),
		StatusCode: 200,
	}, nil
}

func main() {
	sess, err := session.NewSession(&aws.Config{
		Region: aws.String("eu-west-1"),
	})

	if err != nil {
		log.Fatalf("failed to create AWS session, %v", err)
	}

	s3 := s3.New(sess)
	app := App{S3: s3}
	lambda.Start(app.handler)
}
