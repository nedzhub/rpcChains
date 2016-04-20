// Usage: go run client.go [client ip:port] [front-end ip:port]
//
// - [client ip:port] : the ip and TCP port on which this client is listening for file server connections.         
// - [front-end ip:port] : the ip and TCP port on which front end is listening for client connections. 
//
package main

import (
	"fmt"
	"time"
	"net/rpc"
	"os"
	"net"
	"log"
	//"io"
	"bufio"
	"strconv"
	"strings"
	"io/ioutil"
	"github.com/arcaneiceman/GoVector/govec"

)
//======================================= SERVICE =======================================
type ClientService int

//======================================= STRUCTS =======================================
type ValSecret struct {
	Val string // value; depends on the call
}

type ValContent struct {
	Val string // value; depends on the call
}

type ValArgs struct{
	File_Name string
	Text_content string
	Secret_info string
}


type ValReply struct {
	Val string // value; depends on the call
	Log LogArg
}

type EntryFunction struct{
	Store string
	Retrieve string
	List string
}

type ServerEntity struct {
	Connection_info string
	Service_info string
	Entry EntryFunction
}

type RPCChain struct {
	Id int
	CurrrentPosition int
	Chain []ServerEntity
	Args ValArgs
	Log LogArg
}

type NodeInfoCache struct {
	Id int
	Type int
	Addr string
	Service string
	Entry EntryFunction
	Maps []map[string]string
}

type NodeInfo struct {
	Id int
	Type int
	Addr string
	Service string
	Entry EntryFunction
}

type CacheContent struct{
	Maps []map[string]string
}

// GOVEC
type LogArg struct{
	Log []byte
}

type Msg struct {
	Content string 
	RealTimestamp string
}

//======================================= VARIABLES =======================================
const NodeType int = 0
const NodeService string = "ClientService"
const StoreEntryFunction string = "CStore"
const RetrieveEntryFunction string = "CRetrieve"
const ListEntryFunction string = "CList"
const MAX_FILE_SIZE = 10240

var callBackChainID int
var callBackContent string
const FileBasePath = "./ClientFiles/"

var Logger *govec.GoLog

//======================================= SERVICE METHODS =======================================


func (cs *ClientService) CStore(rpcc *RPCChain, reply *ValReply) error {

	reply.Val="Client received STORE info"
	fmt.Println("FILE NAME:",rpcc.Args.File_Name, "SECRET:", rpcc.Args.Secret_info)
	fmt.Println("File succesfully stored, terminating.") 
	callBackChainID = rpcc.Id
	return nil
}

func (cs *ClientService) CRetrieve(rpcc *RPCChain, reply *ValReply) error {
	reply.Val="Client received RETRIEVE info"
	
	//fmt.Println("FILE NAME:",rpcc.Args.File_Name, "CONTENT:", rpcc.Args.Text_content)   //for testing, prints entire files content
		
	if(rpcc.Args.File_Name != ""){
		fmt.Println("File succesfully retrieved, terminating.") 
		saveFile(rpcc.Args.File_Name,rpcc.Args.Text_content)
	}else{
		fmt.Println("File not found.") 
	}

	callBackChainID = rpcc.Id
	callBackContent = rpcc.Args.Text_content
	return nil
}

func (cs *ClientService) CList(rpcc *RPCChain, reply *ValReply) error {
	reply.Val="Client received LIST info"
	fmt.Println(rpcc.Args.File_Name)
	fmt.Println("File succesfully listed, terminating.") 

	return nil
}


//======================================= MAIN =======================================

func main() {

	Logger = govec.Initialize("client", "clientlogfile")
	// parse args
	usage := fmt.Sprintf("Usage: %s ip:port\n", os.Args[0])
	if len(os.Args) != 3 {
		fmt.Printf(usage)
		os.Exit(1)
	}

	clientAddr := os.Args[1]
	frontendAddr := os.Args[2]
	fmt.Println("clientAddr:", clientAddr, " frontendAddr:", frontendAddr)


	go initListener(clientAddr)
	serviceFE, _ := rpc.Dial("tcp", frontendAddr)
	
	// //fmt.Println(serviceFE)
	// //checkError(err)

	// name := "myfile3.txt"
	// content := "sample text"
	// name2 := "myfile2222223.txt"
	// content2 := "sample text 222222"
	// secret := "pass"

	// chainID,_:= strconv.Atoi(Handshake(serviceFE))
	// fmt.Println("STORE 1=================")
	// ServiceCall(chainID, "store", name, content, NodeType, clientAddr, secret, serviceFE)
	// fmt.Println()
	// //time.Sleep(1000 * time.Millisecond)
	// fmt.Println("RETRIEVE 1=================")
	// ServiceCall(chainID, "retrieve", name, "", NodeType, clientAddr, secret, serviceFE)
	// fmt.Println()

	// chainID,_= strconv.Atoi(Handshake(serviceFE))
	// fmt.Println("STORE 2=================")
	// ServiceCall(chainID, "store", name2, content2, NodeType, clientAddr, "", serviceFE)
	// fmt.Println()
	// fmt.Println("RETRIEVE 2=================")
	// ServiceCall(chainID, "retrieve", name2, "", NodeType, clientAddr, "", serviceFE)
	// fmt.Println()
	
	// chainID,_= strconv.Atoi(Handshake(serviceFE))
	// fmt.Println("LIST ===============")
	// ServiceCall(chainID, "list", "", "", NodeType, clientAddr, "", serviceFE)

	// Constantly reads for new commands.	
	for {
		reader := bufio.NewReader(os.Stdin)
		fmt.Println("Enter a command: [type help for options]");
		cmd, _ := reader.ReadString('\n')

		// Check if the command is valid.
		handleCommand(cmd,clientAddr,serviceFE)
	}

}



//======================================= HELPER FUNCTIONS =======================================
func (m Msg) String()  string {
	return "content: " + m.Content + "\ntime: " + m.RealTimestamp
}

// This function handles the command entered by the user.
func handleCommand(command string, clientAddr string, serviceFE *rpc.Client) {

	result := strings.Split(command, " ")

	// Set the command.
	cmd := result[0]
	cmd = strings.TrimSpace(cmd)
	secret := ""
	strContent := ""

	// Set the file.
	fname := ""
	if len(result) == 2 {
		fname = strings.TrimSpace(result[1])  //
	} else if len(result) == 3 {
		fname = strings.TrimSpace(result[1])
		secret = strings.TrimSpace(result[2])
	}

	filename := strings.TrimSpace(fname)

	switch cmd {	
		// GET
		case "retrieve": 
		chainID,_:= strconv.Atoi(Handshake(serviceFE))
		if filename == "" {
			fmt.Println("Please provide a file name.")
			return
		}
		retrieveMessage := Msg{"Client retrieve log", time.Now().String()}
		retrieveBuf := Logger.PrepareSend("Retrieve request to frontend", retrieveMessage)
		retrieveLog := LogArg{retrieveBuf}
		fmt.Println("for retrieve testing: ", secret, "length is: ", len(secret)) //test to see if retrieve working correctly with reading inputs
		ServiceCall(chainID, "retrieve", filename, "", NodeType, clientAddr, secret, serviceFE, retrieveLog)
		fmt.Println("Retrieve in SWITCH CALLED")

		// PUT
		case "store": 
		fmt.Println(filename)
		chainID,_:= strconv.Atoi(Handshake(serviceFE))
		if filename == "" {
			fmt.Println("Please provide a file name.")
			return
		}
		storeMessage := Msg{"Client store log", time.Now().String()}
		storeBuf := Logger.PrepareSend("Store request to frontend", storeMessage)
		storeLog := LogArg{storeBuf}
		strContent = readFile(filename)
		fmt.Println("secret for this store call is ", secret, "length is: ", len(secret)) //for testing secret
		ServiceCall(chainID, "store", filename, strContent, NodeType, clientAddr, secret, serviceFE, storeLog)
		fmt.Println("Store in SWITCH CALLED")

		// LISTGET
		case "listget": 
		chainID,_:= strconv.Atoi(Handshake(serviceFE))
		listMessage := Msg{"Client list log", time.Now().String()}
		listBuf := Logger.PrepareSend("List request to frontend", listMessage)
		listLog := LogArg{listBuf}
		fmt.Println("LISTGET  BEFORE SERVICE CALL")
		ServiceCall(chainID, "list", "", "", NodeType, clientAddr, "", serviceFE, listLog)
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


func readFile(filename string) string {

	fmt.Println(FileBasePath+filename)

	rfbuf,_ := ioutil.ReadFile(FileBasePath+filename) 
	fmt.Println("size of rf is :" , len(rfbuf))

	strContent := string(rfbuf)

	return strContent

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


//initiate connection to front end to get my ChainID
func Handshake(dialservice *rpc.Client) string{

	args:= ValReply {
		Val: "hello", 
	}
	var kvVal ValReply

	err := dialservice.Call("FrontEndServiceClient.Handshake", args, &kvVal)
	checkError(err)

	return kvVal.Val
}


// send unavailable Keys and waitList to make map
func ServiceCall(id int, callType string, name string, content string, index int, address string, secret string, dialservice *rpc.Client, outLog LogArg){

	entryFunc := EntryFunction {
		Store: StoreEntryFunction,
		Retrieve: RetrieveEntryFunction,
		List: ListEntryFunction,
	}

	entity := ServerEntity {
		Connection_info: address,
		Service_info: NodeService,
		Entry: entryFunc,
	}

	entitylist:= make([]ServerEntity,1)
	entitylist[index]=entity


	arg := ValArgs{
		File_Name: name,
		Text_content: content,
		Secret_info: secret,
	}

	rpcc := RPCChain{
		Id: id,
		CurrrentPosition: index,
		Chain: entitylist,
		Args: arg,
		Log: outLog,
	}

	var kvVal ValReply

	fmt.Println("Calling front end with chain:", rpcc)
	switch callType {
		case "store":
			err := dialservice.Call("FrontEndServiceClient.FStore", rpcc, &kvVal)
			checkError(err)
			fmt.Println("Front end STORE reply:", kvVal.Val)
		case "retrieve":
			err := dialservice.Call("FrontEndServiceClient.FRetrieve", rpcc, &kvVal)
			checkError(err)
			fmt.Println("Front end RETRIEVE reply:", kvVal.Val)
		case "list":
			fmt.Println("before actually do call")
			err := dialservice.Call("FrontEndServiceClient.FList", rpcc, &kvVal)
			checkError(err)
			fmt.Println("Front end LIST reply:", kvVal.Val)
			fmt.Println("got it all")
			
	}
	
}



//write the given file to client's file folder 
func saveFile(filename string, filecontent string) {
	byteContent := []byte(filecontent)
	err := ioutil.WriteFile(FileBasePath+filename,byteContent,0644)
	if err != nil {
		fmt.Println("Write failed for: ", filename)
	}
}


//not being used currently
// PUT command
func putFile(file string) {

	result := strings.TrimSpace(file)
	fmt.Println(FileBasePath+result)
	f, err:= os.Open(FileBasePath+result)
	checkError(err)

	reader := bufio.NewReader(f)

	buf := make([]byte, 1024)
	_, err = reader.Read(buf)
	checkError(err)
	fmt.Println(string(buf));

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

