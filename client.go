// Usage: go run client.go [client ip:port] [front-end ip:port]
//
// - [client ip:port] : the ip and TCP port on which this client is listening for file server connections.
// - [front-end ip:port] : the ip and TCP port on which front end is listening for client connections.
//
package main 

import (
	"./rpcc"
	"bufio"
	"encoding/gob"
	"fmt"
	"github.com/arcaneiceman/GoVector/govec"
	"io/ioutil"
	"log"
	"net"
	"net/rpc"
	"os"
	"strings"
	"time"
)

//======================================= SERVICE =======================================
type ClientService int

//======================================= STRUCTS =======================================
type ValArgs struct {
	File_Name    string
	Secret_info  string
	Text_content string
	ErrorCode    int
}

type ValMetadata struct {
	FilestoreMapA map[int]NodeInfo
	FilestoreMapB map[int]NodeInfo
}

type NodeInfo struct {
	Id      int
	Type    int
	Addr    string
	Service string
}

type ValReply struct {
	Val string // value; depends on the call
}

type Msg struct {
	Content       string
	RealTimestamp string
}

//======================================= VARIABLES =======================================
const NodeService string = "ClientService"
const StoreEntryFunction string = "CStore"
const RetrieveEntryFunction string = "CRetrieve"
const ListEntryFunction string = "CList"

const (
	INCOMPLETE_CHAIN     = iota
	SUCCESSFUL_COMPLETED = iota
	INVALID_AUTH_ERROR   = iota
)

var NodeAddress string = ""
var callBackChainID string
var callBackContent string

const FileBasePath = "./ClientFiles/"

var Logger *govec.GoLog

//======================================= SERVICE METHODS =======================================

func (cs *ClientService) CStore(chain *rpcc.RPCChain, reply *bool) error {

	cmd := "STORE"
	HandleLog(cmd, chain)

	args := chain.CurrentEntity().Args.(ValArgs)

	if args.ErrorCode == SUCCESSFUL_COMPLETED {
		fmt.Println("FILE NAME:", args.File_Name, "SECRET:", args.Secret_info)
		fmt.Println("File succesfully stored, terminating.")
		callBackChainID = chain.Id
	} else if args.ErrorCode == INVALID_AUTH_ERROR {
		fmt.Println("Unable to store. Invalid secret provided!")
	} else {
		fmt.Println("Unexpected error during store: ", args.ErrorCode)
	}

	*reply = true
	return nil
}

func (cs *ClientService) CRetrieve(chain *rpcc.RPCChain, reply *bool) error {

	cmd := "RETRIEVE"
	HandleLog(cmd, chain)

	args := chain.CurrentEntity().Args.(ValArgs)

	fmt.Println("FILE NAME:", args.File_Name, "CONTENT:", args.Text_content)
	fmt.Println("File succesfully retrieved, terminating.")

	saveFile(args.File_Name, args.Text_content)

	callBackChainID = chain.Id
	callBackContent = args.Text_content

	*reply = true
	return nil
}

func (cs *ClientService) CList(chain *rpcc.RPCChain, reply *bool) error {

	cmd := "LIST"
	HandleLog(cmd, chain)

	args := chain.CurrentEntity().Args.(ValArgs)

	fmt.Println(args.File_Name)
	fmt.Println("File succesfully listed, terminating.")

	*reply = true
	return nil
}

//======================================= MAIN =======================================

func main() {

	// Initiate Logger
	Logger = govec.Initialize("client", "client")

	// parse args
	usage := fmt.Sprintf("Usage: %s ip:port\n", os.Args[0])
	if len(os.Args) != 3 {
		fmt.Printf(usage)
		os.Exit(1)
	}

	gob.Register(ValArgs{})
	gob.Register(ValMetadata{})
	gob.Register(NodeInfo{})

	clientAddr := os.Args[1]
	frontendAddr := os.Args[2]
	fmt.Println("clientAddr:", clientAddr, " frontendAddr:", frontendAddr)

	//go initListener(clientAddr)

	//name := "myfile3.txt"
	//content := "sample text"
	//name2 := "myfile2222223.txt"
	//content2 := "sample text 222222"
	//secret := "pass"

	go initListener(clientAddr)
	NodeAddress = clientAddr

	//fmt.Println("STORE 1=================")
	//ServiceCall("FStore", name, content, secret, frontendAddr, StoreEntryFunction)
	//fmt.Println()
	//time.Sleep(1000 * time.Millisecond)
	//fmt.Println("RETRIEVE 1=================")
	//ServiceCall("FRetrieve", name, "", secret, frontendAddr, RetrieveEntryFunction)
	//fmt.Println()

	//fmt.Println("STORE 2=================")
	//ServiceCall("FStore", name2, content2, "", frontendAddr, StoreEntryFunction)
	//fmt.Println()
	//fmt.Println("RETRIEVE 2=================")
	//ServiceCall("FRetrieve", name2, "", "", frontendAddr, RetrieveEntryFunction)
	//fmt.Println()

	//fmt.Println("LIST ===============")
	//ServiceCall("FList", "", "", "", frontendAddr, ListEntryFunction)

	// Constantly reads for new commands.
	for {
		reader := bufio.NewReader(os.Stdin)
		fmt.Println("Enter a command: [type help for options]")
		cmd, _ := reader.ReadString('\n')

		// Check if the command is valid.
		handleCommand(cmd, frontendAddr)
	}

}

//======================================= HELPER FUNCTIONS =======================================
func (m Msg) String() string {
	return "content: " + m.Content + "\ntime: " + m.RealTimestamp
}

// This function handles the command entered by the user.
func handleCommand(command string, frontendAddr string) {

	result := strings.Split(command, " ")

	// Set the command.
	cmd := result[0]
	cmd = strings.TrimSpace(cmd)
	secret := ""
	strContent := ""

	// Set the file.   NEW
	fname := ""
	if len(result) == 2 {
		fname = strings.TrimSpace(result[1]) //
	} else if len(result) == 3 {
		fname = strings.TrimSpace(result[1])
		secret = strings.TrimSpace(result[2])
	}

	// var filename string
	filename := strings.TrimSpace(fname) //NEW

	switch cmd {
	// GET
	case "retrieve":
		if filename == "" {
			fmt.Println("Please provide a file name.")
			return
		}
		fmt.Println("for retrieve testing: ", secret, "length is: ", len(secret)) //test to see if retrieve working correctly with reading inputs
		//ServiceCall("FRetrieve", filename, "", NodeType, clientAddr, secret, serviceFE, retrieveLog)  //govec old version
		ServiceCall("FRetrieve", filename, "", secret, frontendAddr, RetrieveEntryFunction)
		fmt.Println("Retrieve in SWITCH CALLED")

	// PUT
	case "store":
		fmt.Println(filename)
		if filename == "" {
			fmt.Println("Please provide a file name.")
			return
		}
		strContent = readFile(filename)
		fmt.Println("strcontent is: ", strContent)
		if strContent == "" {
			fmt.Println("Files cannot be empty. Limit is 1 Mb")
			return
		}
		fmt.Println("secret for this store call is ", secret, "length is: ", len(secret)) //for testing secret
		//ServiceCall(chainID, "store", filename, strContent, NodeType, clientAddr, secret, serviceFE, storeLog)   //old version
		ServiceCall("FStore", filename, strContent, secret, frontendAddr, StoreEntryFunction)

		fmt.Println("Store in SWITCH CALLED")

	// LISTGET
	case "listget":
		fmt.Println("LISTGET  BEFORE SERVICE CALL")
		//ServiceCall(chainID, "list", "", "", NodeType, clientAddr, "", serviceFE, listLog)  //old way
		listFileGet(frontendAddr)
		fmt.Println("LISTGET in SWITCH CALLED")

	// LISTPUT
	case "listput":
		listFilePut()

	case "help":
		fmt.Println("listput -to print local files")
		fmt.Println("listget -to print remote files")
		fmt.Println("retrieve filename [Optional 'Yes' for secure files] -to retrieve a remote file")
		fmt.Println("store filename [Optional Password] -to store a local file")

	// DEFAULT
	default:
		fmt.Println("Invalid command.")
		return
	}
}

// If error is non-nil, print it out and halt.
func checkError(err error) {
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error ", err.Error())
		os.Exit(1)
	}
}

// Register RPC services and listens for incoming dialing
func initListener(address string) {
	service := new(ClientService)
	rpc.Register(service)
	ln, e := net.Listen("tcp", address)
	if e != nil {
		log.Fatal("listen error:", e)
	} else {
		go rpc.Accept(ln)
	}
}

// send unavailable Keys and waitList to make map
func ServiceCall(callType string, name string, content string, secret string, address string, entryFunc string) *rpcc.RPCChain {
	args := ValArgs{
		File_Name:    name,
		Text_content: content,
		Secret_info:  secret,
		ErrorCode:    INCOMPLETE_CHAIN,
	}

	chain := rpcc.CreateChain()

	chain.AddToChain(NodeAddress, NodeService, entryFunc, args, -1)
	chain.AddToChain(address, "FrontEndServiceClient", callType, nil, -1)
	logBuf := GenerateLog(callType, chain)
	chain.AddLogToChain(logBuf)

	//fmt.Println("Calling front end with chain:", chain)
	chain.CallNext(10000)

	return chain
}

func HandleLog(cmd string, chain *rpcc.RPCChain) {

	if chain.IsReturnCall == false {
		logMessage := new(Msg)
		prevChain := chain.CurrentPosition - 1
		Logger.UnpackReceive(cmd+" request received from "+chain.EntityList[prevChain].Service_info, chain.Log, &logMessage)
		fmt.Println(logMessage.String())
	}

	if chain.IsReturnCall == true {
		logMessage := new(Msg)
		prevChain := chain.FindEntity("FrontEndServiceClient")
		Logger.UnpackReceive(cmd+" return from "+prevChain.Service_info, chain.Log, &logMessage)
		fmt.Println(logMessage.String())
	}

}

func GenerateLog(callType string, chain *rpcc.RPCChain) []byte {

	nextChain := chain.CurrentPosition + 1
	logMessage := Msg{"Client " + callType + "log.", time.Now().String()}
	fmt.Println(chain.EntityList[nextChain].Service_info)
	logBuf := Logger.PrepareSend(callType+" request to "+chain.EntityList[nextChain].Service_info, logMessage)
	return logBuf

}

//write the given file to client's file folder
func saveFile(filename string, filecontent string) {
	byteContent := []byte(filecontent)
	err := ioutil.WriteFile(FileBasePath+filename, byteContent, 0644)
	if err != nil {
		fmt.Println("Write failed for: ", filename)
	}
}

// LIST command that returns a list of files that can be retrieved from filestore.
func listFileGet(frontendAddr string) {
	fmt.Println("LIST ===============")
	ServiceCall("FList", "", "", "", frontendAddr, ListEntryFunction)
}

//not being used currently
// PUT command
func putFile(file string) {

	result := strings.TrimSpace(file)
	fmt.Println(FileBasePath + result)
	f, err := os.Open(FileBasePath + result)
	checkError(err)

	reader := bufio.NewReader(f)

	buf := make([]byte, 1024)
	_, err = reader.Read(buf)
	checkError(err)
	fmt.Println(string(buf))

}

//func returns a list of all the files in that directory ClientFiles which has all the client's files
//*assumption that the file is being run from the parent directory
func listFilePut() {

	var fileList []string

	files, err := ioutil.ReadDir(FileBasePath)
	checkError(err)

	for _, f := range files {
		fileList = append(fileList, f.Name())
	}

	if len(fileList) == 0 {
		fmt.Println("No files found.")
	} else {
		for i := range fileList {
			fmt.Println(fileList[i])
		}
	}

}

//reads given filename and returns the file content in string format
//if file is empty or too large, returns early with empty tring
func readFile(filename string) string {

	fmt.Println(FileBasePath + filename)

	rfbuf, _ := ioutil.ReadFile(FileBasePath + filename)
	fmt.Println("size of rf is :", len(rfbuf))
	if len(rfbuf) > 10240 || len(rfbuf) == 0 {
		badFile := ""
		return badFile
	}

	strContent := string(rfbuf)

	return strContent

}
