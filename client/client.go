// socket-client project main.go
package main

import (
	"bufio"
	_ "encoding/json"
	. "fmt"
	"io"
	"io/ioutil"
	"net"
	"os"
	_ "os/exec"
	"regexp"
	"strconv"
	"strings"
)

const (
	SERVER_HOST = "localhost"
	SERVER_PORT = "9988"
	SERVER_TYPE = "tcp"
)

// var name = ""
// var status = "not busy"
// var job_logs []string = make([]string, 0)

var client_key = "123"

func main() {
	//establish connection
	connection, buffer := initNode()
	defer connection.Close()
	go readMessage(connection)
	for {
		doJob(buffer, connection)
	}
}

func doJob(buffer []byte, connection net.Conn) {
	var message string
	_ = message
	job_id := 1
	for {
		println("enter command :::: ")
		scanner := bufio.NewScanner(os.Stdin)
		for scanner.Scan() {
			message = scanner.Text()
			break
		}
		parseCommand(message, connection, job_id)
		job_id++
	}
}

func fillString(retunString string, toLength int) string {
	for {
		lengtString := len(retunString)
		if lengtString < toLength {
			retunString = retunString + ":"
			continue
		}
		break
	}
	return retunString
}

func parseCommand(message string, connection net.Conn, job_id int) {
	if strings.HasPrefix(message, "wlf job add") {
		job_name := strings.Split(string(message), " ")[3]
		jsonFile, err := os.Open("./jobs/" + job_name)
		byteValue, _ := ioutil.ReadAll(jsonFile)
		mm := string(byteValue[:])
		executable := ""
		if strings.Contains(message, "-executable") {
			executable = strings.Split(string(message), " ")[5]
		}
		resource := "0"
		var re = regexp.MustCompile(`(?m)-resource (\d+)`)
		if strings.Contains(message, "-resource") {
			subStr := re.FindStringSubmatch(message)
			resource = subStr[1]
		}
		meta := "{ \n\"job_id\" : [\"" + strconv.Itoa(job_id) + "\"], \n \"job\" : [\"add job\"], \n" + `"executable":["` + executable + `"],` + "\n" + `"resource":["` + resource + `"],` + "\n"
		t := strings.Replace(mm, "{", meta, -1)
		connection.Write([]byte(t))
		jsonFile.Close()
		if executable != "" {
			file, err := os.Open("./exec/" + executable)
			if err != nil {
				println("problem in reading file")
				return
			}
			fileInfo, err := file.Stat()
			fileSize := fillString(strconv.FormatInt(fileInfo.Size(), 10), 10)
			fileName := fillString(fileInfo.Name(), 64)
			println("Sending filename and filesize!")
			connection.Write([]byte(fileSize))
			connection.Write([]byte(fileName))
			sendBuffer := make([]byte, 1024)
			for {
				_, err = file.Read(sendBuffer)
				if err == io.EOF {
					break
				}
				connection.Write(sendBuffer)
			}
		}
		if err != nil {
			Println("Error sending:", err.Error())
		}
	} else if strings.HasPrefix(message, "wlf job list") {
		connection.Write([]byte(`{"job":["list"]}`))
	} else if strings.HasPrefix(message, "wlf job logs") {
		tailHead := ""
		count := ""
		job_id := strings.Split(string(message), " ")[3]
		if len(strings.Split(string(message), " ")) == 7 {
			tailHead = strings.Split(string(message), " ")[4]
			count = strings.Split(string(message), " ")[6]
		}
		t := `{"job":["logs"],"job_id":["` + job_id + `"], "tailHead":["` + tailHead + `"], "count":["` + count + `"]}`
		connection.Write([]byte(t))
	} else if strings.HasPrefix(message, "wlf node list") {
		connection.Write([]byte(`{"node":["list"]}`))
	} else if strings.HasPrefix(message, "wlf node top") {
		connection.Write([]byte(`{"node":["top"]}`))
	} else {
		println("Invalid Command!!!")
	}
}

func initNode() (net.Conn, []byte) {
	println("enter connection key :::: ")
	scanner := bufio.NewScanner(os.Stdin)
	for scanner.Scan() {
		client_key = scanner.Text()
		break
	}
	connection, err := net.Dial(SERVER_TYPE, SERVER_HOST+":"+SERVER_PORT)
	if err != nil {
		panic(err)
	}
	buffer := make([]byte, 1024)
	_, err = connection.Write([]byte("Client to Master key : " + client_key))
	if err != nil {
		Println("err")
	}
	return connection, buffer
}

func readMessage(connection net.Conn) {
	buffer := make([]byte, 1024)
	for {
		mLen, err := connection.Read(buffer)
		if err != nil {
			Println("Error reading:", err.Error())
			break
		}
		Println(string(buffer[:mLen]))
		if string(buffer[:mLen]) == "Wrong key not authenticated" {
			os.Exit(1)
		}
	}
}
