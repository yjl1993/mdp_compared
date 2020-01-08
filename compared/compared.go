package compared

import (
	"encoding/json"
	"fmt"
	client "github.com/influxdata/influxdb1-client/v2"
	"mdp_compared/query"
	"mdp_compared/synchronize"
	"reflect"
	"time"
)

func ComparedCQ(address1, address2, address3 string, port int, dbname,
	msname, cqmsname, starttime, endtime, groupbytime, model string)  {

	bp, _ := client.NewBatchPoints(client.BatchPointsConfig{
		Database: dbname,
	})

	cqcount1, _ := query.QueryDurTime(address1,port,dbname,cqmsname,starttime,endtime)
	cqcount2, isEmpty2 := query.QueryDurTime(address2,port,dbname,cqmsname,starttime,endtime)
	//cqcount3, isEmpty3 := query.QueryDurTime(address3,port,dbname,cqmsname,starttime,endtime)
	fmt.Printf("Start comparing %s and %s. \n", address1, address2)
	match1To2 := reflect.DeepEqual(cqcount1, cqcount2)
	if match1To2 == false {
		Compared(address1, address2, port, dbname, msname, groupbytime, isEmpty2, cqcount1, cqcount2, bp)
		synchronize.SynchronizeCq(address1,address2,port,dbname,cqmsname,starttime,endtime,bp)
		fmt.Println("End of synchronization")
	} else {
		fmt.Printf("Node %s and node %s are synchronized. \n", address1, address2)
	}

	//match1To3 := reflect.DeepEqual(cqres1, cqcount3)
	//if match1To3 == false {
	//	fmt.Printf("Start syncing %s and %s. \n", address1, address3)
	//	Compared(address1, address3, port, dbname, msname, isEmpty3, cqres1, cqcount3, bp)
	//	synchronize.SynchronizeCq(address1,address3,port,dbname,cqmsname,starttime,endtime,bp)
	//	fmt.Println("End of synchronization")
	//} else {
	//	fmt.Printf("Node %s and node %s are synchronized. \n", address1, address3)
	//}
}

func Compared(address1, address2 string, port int, dbname, msname, groupbytime string, isemptyB bool,
	cqcountlistA, cqcountlistB [][]interface{}, bp client.BatchPoints) {
		addr := fmt.Sprintf("%s:%d", address2, port)
		c, err := client.NewHTTPClient(client.HTTPConfig{
			Addr: addr,
		})
		if err != nil {
			fmt.Println("Error creating InfluxDB Client: ", err.Error())
		}
		defer c.Close()
		if isemptyB { //如果这个时间段B节点的count是空的，不用比较，直接将节点A的数据写入B节点中
			for i := 0; i < len(cqcountlistA); i++ { //[[2019-12-19T06:31:40Z 14335] [2019-12-19T06:32:00Z 151590]]
				countAStr, err := json.Marshal(cqcountlistA[i][1])
				if err != nil {
					fmt.Println("生成json字符串失败",err)
				}
				if string(countAStr) != "0" { //这个时间段A节点不为0的
					diffStaTimeStr := cqcountlistA[i][0].(string)
					diffEndTime, _ := time.Parse(time.RFC3339,diffStaTimeStr)
					durtime, _ := time.ParseDuration(groupbytime)
					diffEndTimeStr := diffEndTime.Add(durtime).Format(time.RFC3339)
					fmt.Println("\n",diffStaTimeStr,":::::",diffEndTimeStr)
					start := time.Now()
					synchronize.Synchronize(address1, address2, port, dbname, msname,
						diffStaTimeStr, diffEndTimeStr, c, bp)
					end := time.Now()
					fmt.Println("Synchronize1",string(countAStr),"used time:",end.Sub(start))
				}
			}
		} else {
			cqcountlistAMap := make(map[string]interface{},len(cqcountlistA))
			for i := 0; i < len(cqcountlistA); i++ {
				tagstr, _ := json.Marshal(cqcountlistA[i])
				cqcountlistAMap[string(tagstr)] = cqcountlistA[i]
			}

			cqcountlistBMap := make(map[string]interface{},len(cqcountlistB))
			for i := 0; i < len(cqcountlistB); i++ {
				tagstr, _ := json.Marshal(cqcountlistB[i])
				cqcountlistBMap[string(tagstr)] = cqcountlistB[i]
			}

			for i := 0; i < len(cqcountlistA); i++ {
				//判断两个节点这个时间段哪个COUNT数不一致，并拿到该count数起止时间戳
				countAStr, err := json.Marshal(cqcountlistA[i][1])
				if err != nil {
					fmt.Println("生成json字符串失败",err)
				}
				if string(countAStr) != "0" {
					valuesListAmaptag, _ := json.Marshal(cqcountlistA[i])
					if _, ok := cqcountlistBMap[string(valuesListAmaptag)]; ok {
						continue
					}else {
						diffStaTimeStr := cqcountlistA[i][0].(string)
						diffEndTime, _ := time.Parse(time.RFC3339,diffStaTimeStr)
						durtime, _ := time.ParseDuration(groupbytime)
						diffEndTimeStr:= diffEndTime.Add(durtime).Format(time.RFC3339)
						fmt.Println("\n",diffStaTimeStr,":::::",diffEndTimeStr)
						start := time.Now()
						synchronize.Synchronize(address1, address2, port, dbname, msname,
							diffStaTimeStr, diffEndTimeStr, c, bp)
						end := time.Now()
						fmt.Println("Synchronize2",string(countAStr),"used time:",end.Sub(start))
					}
				}
			}
		}
	}