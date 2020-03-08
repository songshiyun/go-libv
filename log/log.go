package log

import (
	log "github.com/sirupsen/logrus"
	"os"
)

func init()  {
	log.SetFormatter(&log.JSONFormatter{
		TimestampFormat:"2006-01-02 15:04:05.999",
	})
	log.SetOutput(os.Stdout)
	log.SetLevel(log.InfoLevel)
}

