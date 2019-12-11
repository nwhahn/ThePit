package common

import (
	"database/sql"
	"fmt"
	_ "github.com/lib/pq"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/viper"
	_ "github.com/spf13/viper"
	"os"
	"path/filepath"
)

type loginInfo struct {
	dbtype string
	user string
	password string
	database string
	host string
	port int
}

var dbLogin loginInfo

type Database interface {
	InitDb ()
	Execute ()
}

/*
we are going to be lazy and just open a connection and close it per query
 */
func Execute(query string) *sql.Rows {
	log.Info(query)

	connStr := fmt.Sprintf("%s://%s:%s@%s:%d/%s?sslmode=disable", dbLogin.dbtype, dbLogin.user, dbLogin.password, dbLogin.host, dbLogin.port, dbLogin.database)
	log.Info("Creating connection with string:", connStr)
	db, dbErr := sql.Open(dbLogin.dbtype, connStr)
	if dbErr != nil {
		log.Error("Fatal error opening connection:", dbErr)
		panic(fmt.Errorf("unable to open connection"))
	}

	defer db.Close()

	queryResult, qErr := db.Query(query)
	if qErr != nil {
		log.Errorf("Error in query statement:", qErr)
		panic(fmt.Errorf("error in query"))
	} else {
		defer queryResult.Close()
	}
	
	return queryResult
}

func InitDb (account string) {
	path, err := os.Getwd()
	if err != nil {
		log.Info(err)
	}

	log.Info("Reading in config from:", path)

	configDir := filepath.Join(path, "DataAggregator/config/database.yaml")

	log.Println("Reading in file:", configDir)
	v := viper.New()
	v.SetConfigName("database")
	v.SetConfigFile(configDir)

	cfgerr := v.ReadInConfig()
	if cfgerr != nil {
		log.Error("Fatal error config file:", cfgerr)
		panic(fmt.Errorf("database config file not found"))
	}

	dbLogin.database = v.GetString(account + ".database")
	dbLogin.dbtype = v.GetString(account + ".type")
	dbLogin.host = v.GetString(account + ".host")
	dbLogin.port = v.GetInt(account + ".port")
	dbLogin.user = v.GetString(account + ".user")
	dbLogin.password = v.GetString(account + ".password")

	log.Info("Creating connection of with values:", dbLogin)

}
