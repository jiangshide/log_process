package main

import (
	"bufio"
	"fmt"
	"io"
	"log"
	"net/url"
	"os"
	"regexp"
	"strconv"
	"strings"
	"time"
)

type Reader interface {
	Read(rc chan []byte)
}

type Writer interface {
	Write(wc chan *Message)
}

type LogProcess struct {
	rc chan []byte
	wc chan *Message
	read Reader
	write Writer
}

type ReadFromFile struct {
	path string
}

type WriteToDb struct {
	source string
}

type Message struct {
	TimeLocal time.Time
	BytesSent int
	Path,Method,Scheme,Status string
	UpstreamTime,RequestTime float64
}

func(r *ReadFromFile)Read(rc chan []byte){
	f,err := os.Open(r.path)
	if err != nil{
		f,_ = os.Create(r.path)
		panic(fmt.Sprintf("open file error:%s",err.Error()))
	}

	//从文件末尾开始逐行读取文件内容
	f.Seek(0,2)
	rd := bufio.NewReader(f)
	for {
		line,err := rd.ReadBytes('\n')
		if err == io.EOF{
			time.Sleep(500*time.Millisecond)
			continue
		}else if err != nil{
			panic(fmt.Sprintf("ReadBytes error:%s",err.Error()))
		}
		rc <- line[:len(line)-1]
	}
}

func (w *WriteToDb) Write(wc chan *Message){
	for v:=range wc{
		fmt.Println(v)
	}
}

/**
	10.20.6.50 - - [06/Mar/2018:12:49:23 +0000] http "GET /foo?query=t HTTP/1.0" 200 2133 "_" "KeepAliveClient" "_" 1.003 1.098

	([\d\.]+)\s+([^ \[]+)\s+([^ \[]+)\s+\[([^\]]+)\]\s+([a-z]+)\s+\"([^"]+)\"\s+(\d{3})\s+(\d+)\s+\"([^"]+)\"\s+\"(.*?)\"\s+\"([\d\.-]+)\"\s+([\d\.-]+)\s+([\d\.-]+)
*/
func (l *LogProcess)Process(){

	r := regexp.MustCompile(`([\d\.]+)\s+([^ \[]+)\s+([^ \[]+)\s+\[([^\]]+)\]\s+([a-z]+)\s+\"([^"]+)\"\s+(\d{3})\s+(\d+)\s+\"([^"]+)\"\s+\"(.*?)\"\s+\"([\d\.-]+)\"\s+([\d\.-]+)\s+([\d\.-]+)`)

	loc,_ := time.LoadLocation("Asia/Shanghai")
	for v:=range l.rc{
		ret := r.FindStringSubmatch(string(v))
		if len(ret) != 14{
			log.Println("FindStringSubmatch fail:",string(v))
			continue
		}

		message := &Message{}
		t,err := time.ParseInLocation("02/Jan/2006:15:04:05 +0000",ret[4],loc)
		if err != nil{
			log.Println("ParseInLocation fail:",err.Error(),ret[4])
		}
		message.TimeLocal = t
		bytesSent,_ :=strconv.Atoi(ret[8])
		message.BytesSent = bytesSent

		//GET /foo?query=t HTTP/1.0
		reqSli := strings.Split(ret[6]," ")
		if len(reqSli) != 3{
			log.Println("strings.Split fail",ret[6])
			continue
		}
		message.Method = reqSli[0]

		u,err := url.Parse(reqSli[1])
		if err != nil{
			log.Println("url.Parse fail:",err)
			continue
		}
		message.Path = u.Path

		message.Scheme = ret[5]
		message.Status = ret[7]

		upStreamTime,_ := strconv.ParseFloat(ret[12],64)
		message.UpstreamTime = upStreamTime

		requestTime,_ := strconv.ParseFloat(ret[13],64)
		message.RequestTime = requestTime

		l.wc <- message
	}
}

/**
	1.写入模块的实现
	1.1.初始化influxdb client
	1.2.从Write Channel中读取监控数据
	1.3.构造数据并写入influxdb
	1.4.Influxdb简介:
	1.4.1.是一个开源的时序型数据库,使用GO编写,被广泛应用于存储系统的监控数据,IoT行业的实时数据等场景,有以下特征：
	1.4.1.1.部署简单,无外部依赖
	1.4.1.2.内置HTTP支持,使用HTTP读写
	1.4.1.3.类SQL的灵活查询(MAX,MIN,SUN等)
	1.4.2.INFLUXDB关键概念
	1.4.2.1.database:数据库
	1.4.2.2.measurement:数据库中的表
	1.4.2.3.points:表里面的一行数据
	1.4.2.3.1.tags:各种有索引的属性
	1.4.2.3.2.fields:各种记录的值
	1.4.2.3.3.time:数据记录的时间戳,也是自动生成的主索引
	1.4.3.1.写入:curl -i -XPOST 'http://10.20.6.50:8089/write?db=mydb' --data-binary 'cpu_usage,host=server01,region=us-west value=0.64 143402023003202002'
	1.4.3.2.读取:curl -G 'http://10.20.6.50:8089/query?pretty=true' --data-urlencode "db=mydb" --data-urlencode "q=SELECT\"value\" FROM \"cpu_usage\" WHERE \"region\"='us-west'
	1.5.1.分析监控需求：
	1.5.1.1.某个协议下的某个请求在某个请求方法的QPS&响应时间&流量
	1.5.1.1.1.Tags:Path,Method,Scheme,Status
	1.5.1.1.2.Fields:UpstreamTime,RequestTime,BytesSent
	1.5.1.1.3.Time:TimeLocal
 */
func main() {
	r:=&ReadFromFile{
		path:"./acc.log",
	}
	w:=&WriteToDb{
		source:"source.db",
	}

	lp := &LogProcess{
		rc :make(chan []byte),
		wc:make(chan *Message),
		read:r,
		write:w,
	}

	go lp.read.Read(lp.rc)
	go lp.Process()
	go lp.write.Write(lp.wc)

	time.Sleep(60*time.Second)
}