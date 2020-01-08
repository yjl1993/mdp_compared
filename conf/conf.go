package conf

import (
	"github.com/BurntSushi/toml"
	"log"
)

type nodeaddr struct {
	NodeAddr1 string
	NodeAddr2 string
	NodeAddr3 string
}

type port struct {
	Port int
}

type worker struct {
	Worker int
}

type database struct {
	DatabaseName string
	Measurement  string
	Cqmeasurement string
}

type mode struct {
	CheckAndSyn string
}

type durtime struct {
	Starttime string
	Endtime   string
}

type groupbytime struct {
	ByTime string
}

type Config struct {
	NodeAddr    nodeaddr
	Port        port
	Worker		worker
	Database    database
	Mode        mode
	Durtime     durtime
	Groupbytime groupbytime
}

func NewToml() Config {
	var conf Config
	var conpath = "./conf.toml"
	if _, err := toml.DecodeFile(conpath, &conf); err != nil {
		log.Fatal(err)
	}
	return conf
}

