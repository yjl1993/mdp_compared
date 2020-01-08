package main

import (
	"flag"
	"time"
	"fmt"

	"mdp_compared/compared"
)

var (
	addr1  		  = ""
	addr2         = ""
	addr3         = ""
	port      	  int
	worker 		  int
	database      = ""
	measurement   = ""
	cqmeasurement = ""
	starttime     = ""
	endtime       = ""
	groupbytime   = ""
	checkandsyn   = ""
)


func init() {
	flag.StringVar(&addr1, "addr1", "", "addr1")
	flag.StringVar(&addr2, "addr2", "", "addr2")
	flag.StringVar(&addr3, "addr3", "", "addr3")
	flag.IntVar(&port, "port", 0, "port")
	flag.IntVar(&worker, "worker", 0, "worker")
	flag.StringVar(&database, "database", "", "database")
	flag.StringVar(&measurement, "measurement", "", "measurement")
	flag.StringVar(&cqmeasurement, "cqmeasurement", "", "cqmeasurement")
	flag.StringVar(&starttime, "starttime", "", "starttime")
	flag.StringVar(&endtime, "endtime", "", "endtime")
	flag.StringVar(&groupbytime, "groupbytime", "", "groupbytime")
	flag.StringVar(&checkandsyn, "checkandsyn", "", "model, if enter 'yes',it will synchronize")
	flag.Parse()
}

func main()  {
	start := time.Now()
	compared.ComparedCQ1(addr1, addr2, addr3, port,worker, database,
		measurement,cqmeasurement, starttime, endtime, groupbytime, checkandsyn)
	end := time.Now()
	fmt.Println("used time:", end.Sub(start))
}
//go run main.go  -addr1="http://192.168.20.132" -addr2="http://192.168.20.133" -addr3="http://192.168.20.135" -database="diff"  -measurement="diff1s_1" -starttime="2020-01-07T09:40:16Z" -endtime="2020-01-07T09:42:40Z"  -checkandsyn="yes" -port=8086 -worker=5

//func main() {
//	conf := conf.NewToml()
//	start := time.Now()
//	compared.ComparedCQ1(conf.NodeAddr.NodeAddr1, conf.NodeAddr.NodeAddr2, conf.NodeAddr.NodeAddr3, conf.Port.Port,
//		conf.Worker.Worker,conf.Database.DatabaseName, conf.Database.Measurement,conf.Database.Cqmeasurement,
//		conf.Durtime.Starttime, conf.Durtime.Endtime, conf.Groupbytime.ByTime, conf.Mode.CheckAndSyn)
//	end := time.Now()
//	fmt.Println("used time:", end.Sub(start))
//}