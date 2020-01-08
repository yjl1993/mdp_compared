package main

import (
	"fmt"
	"github.com/influxdata/influxdb1-client/v2"
	"log"
	"mdp_compared/conf"
	"mdp_compared/query"
	"time"
)

func main()  {
	conf := conf.NewToml()
	addrress := conf.NodeAddr.NodeAddr2
	port := conf.Port.Port
	db   := conf.Database.DatabaseName
	cqmeasurement := conf.Database.Cqmeasurement
	addr := fmt.Sprintf("%s:%d", addrress, port)
	cqcount, _ := query.QueryDurTime(addrress,port,db, cqmeasurement,conf.Durtime.Starttime,conf.Durtime.Endtime)
	fieldKey := query.QueryFieldKeys(addrress,port,db, cqmeasurement)
	c, err := client.NewHTTPClient(client.HTTPConfig{
		Addr: addr,
	})
	if err != nil {
		log.Fatalln("Error: ", err)
	}
	bp, _ := client.NewBatchPoints(client.BatchPointsConfig{
		Database:  db,
	})
	for i := 0; i < len(cqcount); i++ {
		fieldKeyStr := fieldKey[0][0].(string)
		timeStr := cqcount[i][0].(string)
		timeStrParse, _ := time.Parse(time.RFC3339,timeStr)
		tags := make(map[string]string)
		fields := map[string]interface{}{
			fieldKeyStr : 1,
		}
		pt, err := client.NewPoint(cqmeasurement, tags, fields,timeStrParse)
		fmt.Println(pt)
		if err != nil {
			log.Fatalln("Error: ", err)
		}
		bp.AddPoint(pt)
	}
	c.Write(bp)
}
