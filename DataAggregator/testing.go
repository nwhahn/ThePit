package main

import (
	"./common"
	log "github.com/sirupsen/logrus"
)

func main (){
	common.SetupLogging("test")

	log.Info("Testing out calling from another application")

	common.InitDb("bitcoin_writer")
	common.Execute("SELECT MAX(date_values) from bitcoin.bitcoinkeywords")
}