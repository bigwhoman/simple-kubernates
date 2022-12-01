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
	"time"

	"github.com/goombaio/namegenerator"
)

const (
	SERVER_HOST = "localhost"
	SERVER_PORT = "9988"
	SERVER_TYPE = "tcp"
)

var child_nodes map[net.Conn]string = make(map[net.Conn]string, 0)

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

	go getCommands()
	connectToSlaves(server)
}

func getCommands() {
	// for {

	// }
}

func connectToSlaves(server net.Listener) {
	seed := time.Now().UTC().UnixNano()
	nameGenerator := namegenerator.NewNameGenerator(seed)
	for {
		connection, err := server.Accept()
		rndName := nameGenerator.Generate()
		child_nodes[connection] = rndName
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
		var message string
		_ = message
		scanner := bufio.NewScanner(os.Stdin)
		for scanner.Scan() {
			message = scanner.Text()
			break //optional line if your input has a single line
		}
		jsonFile, err := os.Open("./jobs/j2.json")
		defer jsonFile.Close()
		byteValue, _ := ioutil.ReadAll(jsonFile)
		connection.Write(byteValue)
		if err != nil {
			Println("Error sending:", err.Error())
		}
	}
}

func makeJobPool() {

}

func readMessage(connection net.Conn) {
	buffer := make([]byte, 1024)
	for {
		mLen, err := connection.Read(buffer)
		if err != nil {
			Println("Error reading:", err.Error())
			break
		}
		Println("Received ----> ", string(buffer[:mLen]))
	}
}
