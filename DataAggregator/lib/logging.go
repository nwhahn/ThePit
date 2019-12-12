package lib

import (
	"fmt"
	nested "github.com/antonfisher/nested-logrus-formatter"
	log "github.com/sirupsen/logrus"
	"os"
	"os/user"
	"time"
)

func SetupLogging(application string) {
	locUser, err := user.Current()
	if err != nil {
		panic(err)
	}

	filepath := fmt.Sprintf("%s/log/%s_%s.log", locUser.HomeDir, time.Now().Format("2006_01_02_15_04"), application)

	file, err := os.OpenFile(filepath, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0644)
	if err != nil {
		log.Fatal(err)
	}

	log.SetOutput(file)
	log.SetReportCaller(true)

	log.SetFormatter(&nested.Formatter{
		FieldsOrder: []string{"component", "category"},
		HideKeys: true,
		NoColors: true,
	})

}
