package main

import (
	"./lib"
	"database/sql"
	"encoding/json"
	"fmt"
	"github.com/akamensky/argparse"
	_ "github.com/lib/pq"
	"io/ioutil"
	"log"
	"net/http"
	"time"
)

type BpiTotalStruct struct {
	Time       Time
	Disclaimer string
	ChartName  string
	Bpi        Bpi
}

type Bpi struct {
	Usd PricingInfo `json:"USD"`
	Gbp PricingInfo `json:"GBP"`
	Eur PricingInfo `json:"EUR"`
}

type PricingInfo struct {
	Code        string
	Symbol      string
	Rate        string
	Description string
	Rate_float  float64
}

type Time struct {
	Updated    string
	UpdatedISO string
	Udpateduk  string
}

func unjsonify (json_s string) BpiTotalStruct {
	var jsonify BpiTotalStruct
	err := json.Unmarshal([]byte(json_s), &jsonify)
	if err != nil {
		fmt.Println(err)
	}
	return jsonify
}

func getBitcoinJson (url string) string {
	resp, err := http.Get(url)
	if err != nil {
		log.Fatalln(err)
	}

	defer resp.Body.Close()

	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		log.Fatalln(err)
	}

	return string(body)
}

func insertData (bpi *BpiTotalStruct, db *sql.DB, datetime string) {
	query := fmt.Sprintf("INSERT INTO bitcoin.bitcoinpriceindex VALUES ('%s', %f, %f, %f)", datetime, bpi.Bpi.Usd.Rate_float, bpi.Bpi.Eur.Rate_float, bpi.Bpi.Gbp.Rate_float)
	fmt.Println(query)
	insert, err := db.Query(query)
	if err != nil {
		fmt.Println(err)
		fmt.Println("Something has gone wrong with inserting")
	} else {
		defer insert.Close()
		fmt.Println("Successfully inserted rows")
	}
}

func main () {

	lib.SetupLogging("BPIScraper")

	parser := argparse.NewParser("BPIScraper", "Continually scraps the bpi index for bitcoin pricing data every minute")

	url := parser.String("u", "--url",
		&argparse.Options{Default: "https://api.coindesk.com/v1/bpi/currentprice.json", Help: "Url to download bpi values from"})
	outFmt := parser.String("o", "out-fmt",
		&argparse.Options{Default: "2006-01-02 15:04:05", Help: "format of the output datetime?"})
	inFmt := parser.String("i", "in-fmt",
		&argparse.Options{Default: "2006-01-02 15:04:05", Help: "format of the input datetime?"})

	db, err := sql.Open("postgres", "postgres://bitcoin_scripts_writer:kzZkFpNHhRDVqw7Z@192.168.1.5:5432/prod?sslmode=disable")

	if err != nil {
		panic(err.Error())
	}

	defer db.Close()

	for true {
		bpi := unjsonify(getBitcoinJson(*url))
		fmt.Printf("Current time {%s}: Current usd to bitcoin exchange rate: {%s}\n", bpi.Time.Updated, bpi.Bpi.Usd.Rate)
		datetime, err := time.Parse(*inFmt, bpi.Time.UpdatedISO)
		if err != nil {
			fmt.Printf("Could not parse date with error %s", err.Error())
		}
		insertData(&bpi, db, datetime.Format(*outFmt))
		time.Sleep(60 * time.Second)
	}
}
