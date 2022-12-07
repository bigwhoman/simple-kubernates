// socket-server project main.go
package main

import (
	"bufio"
	_ "bufio"
	_ "encoding/json"
	. "fmt"
	"io/ioutil"
	"net"
	"os"
	"strings"
	"time"

	"github.com/goombaio/namegenerator"
)

const (
	SERVER_HOST = "localhost"
	SERVER_PORT = "9988"
	SERVER_TYPE = "tcp"
)

var child_nodes map[string]net.Conn = make(map[string]net.Conn, 0)

var send_commands bool

var runable_files []string = make([]string, 0)

func main() {
	num_of_children := 5
	_ = num_of_children
	Println("Server Running...")
	server, err := net.Listen(SERVER_TYPE, SERVER_HOST+":"+SERVER_PORT)
	if err != nil {
		Println("Error listening:", err.Error())
		os.Exit(1)
	}
	defer server.Close()
	Println("Listening on " + SERVER_HOST + ":" + SERVER_PORT)
	Println("Waiting for client...")
	go connectToSlaves(server)
	getCommands()
}

func makeJobPool() {
	for {
		
	}
}

func getCommands() {
	var message string
	_ = message
	for {
		println("enter command :::: ")
		scanner := bufio.NewScanner(os.Stdin)
		for scanner.Scan() {
			message = scanner.Text()
			break
		}
		parseCommand(message)
	}
}

func parseCommand(message string) {

	if message == "run on children" {
		send_commands = true
	} else if strings.HasPrefix(message, "get logs") {
		child_name := strings.Split(string(message), " ")[2]
		if val, ok := child_nodes[child_name]; ok {
			val.Write([]byte(`{"ack":["logs"]}`))
		} else {
			println("no child with this name !!")
		}
	} else if strings.HasPrefix(message, "exit child") {
		child_name := strings.Split(string(message), " ")[2]
		if val, ok := child_nodes[child_name]; ok {
			val.Write([]byte(`{"ack":["exit"]}`))
		} else {
			println("no child with this name !!")
		}
	} else if message == "get children status" {
		for _, v := range child_nodes {
			v.Write([]byte(`{"ack":["status"]}`))
		}
	} else {
		println("Invalid Command!!!")
	}
}

func connectToSlaves(server net.Listener) {
	seed := time.Now().UTC().UnixNano()
	nameGenerator := namegenerator.NewNameGenerator(seed)
	for {
		connection, err := server.Accept()
		rndName := nameGenerator.Generate()
		child_nodes[rndName] = connection
		if err != nil {
			Println("Error accepting: ", err.Error())
			os.Exit(1)
		}
		Println("client connected")
		go processClient(connection, rndName)
	}
}

func processClient(connection net.Conn, name string) {
	connection.Write([]byte(name))
	go readMessage(connection)
	defer connection.Close()
	for {
		if send_commands {
			jsonFile, err := os.Open("./jobs/j2.json")
			byteValue, _ := ioutil.ReadAll(jsonFile)
			connection.Write(byteValue)
			jsonFile.Close()
			if err != nil {
				Println("Error sending:", err.Error())
			}
			send_commands = !send_commands
		}
	}
}

func readMessage(connection net.Conn) {
	buffer := make([]byte, 1024)
	var child_name string
	for key, v := range child_nodes {
		if v == connection {
			child_name = key
		}
	}
	for {
		mLen, err := connection.Read(buffer)
		if err != nil {
			Println("Error reading:", err.Error())
			delete(child_nodes, child_name)
			break
		}

		Println("Child : ", child_name, " === sent message : ", string(buffer[:mLen]))
	}
}
