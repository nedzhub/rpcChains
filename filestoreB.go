// Usage: go run filestoreB.go [file-store-B ip:port] [frontend ip:port] r
// - [file-store-B ip:port] : the ip and TCP port on which this client is listening for file server connections         
// - [client ip:port] : the ip and TCP port on which client is listening for client connections. 
//
package main

import (
	"fmt"
	"net/rpc"
	"os"
	"time"
	"net"
	"log"
	"strconv"
	"io/ioutil"
	"strings"
	"github.com/arcaneiceman/GoVector/govec"
)

//======================================= SERVICE =======================================
type FilestoreServiceB int

//======================================= STRUCTS =======================================
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

// Type 0 is metadata server, 1 is auth server, 2 is file storage A, 3 is file storage B
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

type LogArg struct{
	Log []byte
}

type Msg struct {
	Content string 
	RealTimestamp string
}
//======================================= VARIABLES =======================================
const NodeType int = 4
const NodeService string = "FilestoreServiceB"
const StoreEntryFunction string = "FBStore"
const RetrieveEntryFunction string = "FBRetrieve"
const ListEntryFunction string = "FBList"

const MetadataService string = "MetadataService"
const FileBasePath = "./StorageFilesB/"


var FileContentMapB map[string]string

var Logger *govec.GoLog

//=========================================== LOGGER ============================================
func HandleLog(callType string, rpcc *RPCChain) {

	prevServer := (rpcc.Chain[rpcc.CurrrentPosition].Service_info)
	logFromPrev := rpcc.Log.Log
	msgFromPrev := new(Msg)

	switch callType {
		case "store":
			Logger.UnpackReceive("Store request from " + prevServer, logFromPrev[0:], &msgFromPrev)
			fmt.Println(msgFromPrev.String())
			return

		case "retrieve":
			Logger.UnpackReceive("Retrieve request from " + prevServer, logFromPrev[0:], &msgFromPrev)
			fmt.Println(msgFromPrev.String())
			return

		case "list":
			Logger.UnpackReceive("List request from " + prevServer, logFromPrev[0:], &msgFromPrev)
			fmt.Println(msgFromPrev.String())
			return
	}
}


//======================================= SERVICE METHODS =======================================
func (fsb *FilestoreServiceB) FBStore(rpcc *RPCChain, reply *ValReply) error {

	var callType = "store"
	HandleLog(callType, rpcc)

	reply.Val = "Data sent from FileStore B STORE"
	fmt.Println("STORE RPCC:")
	updatedPos:=incCurrentPos(*rpcc)

	dbEntity:=containsEntity(updatedPos.Chain,MetadataService)

	if(dbEntity!=ServerEntity{}){
		dbConn,err:=dialAddr(dbEntity.Connection_info)
		if(err==nil){
			nilFlag:=rpcCall("store",updatedPos)
			if(nilFlag != "nil"){
				arg := ValReply {
					Val: updatedPos.Args.File_Name,
				}
				
				var kvVal ValReply
				serviceMethod := dbEntity.Service_info + "." + "StoreValidation"
				err := dbConn.Call(serviceMethod, arg, &kvVal)
				fmt.Println(kvVal.Val)
				checkError(err)
				
				FileContentMapB[updatedPos.Args.File_Name]=updatedPos.Args.Text_content
			}
		}	
	}
	
	fmt.Println(updatedPos)
	return nil
}
func (fsb *FilestoreServiceB) FBRetrieve(rpcc *RPCChain, reply *ValReply) error {

	var callType = "retrieve"
	HandleLog(callType, rpcc)

	reply.Val = "Data sent from FileStore B RETRIEVE"
	fmt.Println("RETRIEVE RPCC:")
	updatedPos:=incCurrentPos(*rpcc)

	if _, ok := FileContentMapB[updatedPos.Args.File_Name]; ok {
		updatedPos.Args.Text_content=FileContentMapB[updatedPos.Args.File_Name]
    } else {
    	updatedPos.Args.File_Name=""
    	updatedPos.Args.Text_content=""
    } 

	rpcCall("retrieve",updatedPos)
	
	fmt.Println(updatedPos)
	return nil
}

func (fsb *FilestoreServiceB) FBList(rpcc *RPCChain, reply *ValReply) error {

	var callType = "list"
	HandleLog(callType, rpcc)

	reply.Val = "Data sent from FileStore B LIST"
	fmt.Println("LIST RPCC:")
	updatedPos:=incCurrentPos(*rpcc)
	// lfg := listFilesGet() //new    //no longer doing it this way

	// updatedPos.Args.File_Name = lfg///new   //old=  "B name1 name2 name3"
	updatedPos.Args.File_Name = "ChangingToMapFilesInMetaB"
	rpcCall("list",updatedPos)
	fmt.Println(updatedPos)
	
	return nil
}

func (fsa *FilestoreServiceB) UpdateConsistency(arg *CacheContent, reply *ValReply) error {
	reply.Val = "Replica updated"
	if (len(arg.Maps) ==1){
		FileContentMapB=arg.Maps[0]
	}
	return nil
}
//======================================= MAIN =======================================

func main() {

	Logger = govec.Initialize("filestoreB", "filestoreB")
	// parse args
	usage := fmt.Sprintf("Usage: %s ip:port\n", os.Args[0])
	if len(os.Args) != 4 {
		fmt.Printf(usage)
		os.Exit(1)
	}

	filestoreAddr := os.Args[1]
	frontendAddr := os.Args[2]
	replicationFactor := os.Args[3]
	fmt.Println("filestoreAddrB:", filestoreAddr, " frontendAddr:",frontendAddr, " replicationFactor:", replicationFactor)

	FileContentMapB = make(map[string]string)

	go initListener(filestoreAddr)
	go printMaps()

	serviceFE, err := rpc.Dial("tcp", frontendAddr)
	checkError(err)
	myID := HandShake(serviceFE)
	UpdateToFrontEnd(myID, NodeType, NodeService, filestoreAddr, serviceFE)
	//counter:=0
	for {
		//counter++
		//fmt.Println(counter)
		serviceFE, err = rpc.Dial("tcp", frontendAddr)
		if err == nil {
			UpdateToFrontEnd(myID, NodeType, NodeService, filestoreAddr, serviceFE)
		}
		time.Sleep(1000 * time.Millisecond)
	}
}

//======================================= HELPER FUNCTIONS =======================================
func (m Msg) String()  string {
	return "content: " + m.Content + "\ntime: " + m.RealTimestamp
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
	service := new(FilestoreServiceB)
	rpc.Register(service)
	ln, e := net.Listen("tcp", address)
	if e != nil {
		log.Fatal("listen error:", e)
	} else {
		go rpc.Accept(ln)
	}
}

func printMaps(){
	for{
		fmt.Println()
		fmt.Println("Printing stored files...")
		fileCount:=1	
		for k,v :=  range FileContentMapB{
			fmt.Println(fileCount,"FILENAME:",k)
			fmt.Println("  CONTENT:",v)
			fileCount++
		}
		time.Sleep(3000*time.Millisecond)
	}
}

// handshake to get my id key in front end map
func HandShake(dialservice *rpc.Client) string{

	outgoingHandshake := Msg{"Hello frontend from FSB", time.Now().String()}
	handshakeBuf := Logger.PrepareSend("Making connection with frontend", outgoingHandshake)
	handshakeLog := LogArg{handshakeBuf}

	args:= ValReply{ 
		Val: "hello",
		Log: handshakeLog,
	}
	var kvVal ValReply

	dialservice.Call("FrontEndServiceFilestoreB.Handshake", args, &kvVal)
	//checkError(err)

	return kvVal.Val
}

// send this node's information to front end 
func UpdateToFrontEnd(id string, typeArg int, service string, address string, dialservice *rpc.Client){

	ID,_:=strconv.Atoi(id)

	entryFunc:= EntryFunction{
		Store: StoreEntryFunction,
		Retrieve: RetrieveEntryFunction,
		List: ListEntryFunction,
	}

	list := make([]map[string]string,1)
	list[0] = FileContentMapB

	node := NodeInfoCache {
		Id: ID,
		Type: typeArg,
		Addr: address, 
		Service: service,
		Entry: entryFunc,
		Maps: list,
	}

	var kvVal ValReply

	dialservice.Call("FrontEndServiceFilestoreB.ReportServerActivity", node, &kvVal)
	//checkError(err)
	//fmt.Println("ReportServerActivity err:",err)
	//fmt.Println("Updated activity status:", node, kvVal.Val)
	
}


//Set current position to +=1
func incCurrentPos(rpcc RPCChain) RPCChain{
	rpcc.CurrrentPosition++
	return rpcc
}

func containsEntity(slice []ServerEntity, s string) ServerEntity{
	for _, v := range slice{
		if v.Service_info == s{
			return v
		}
	}
	return ServerEntity{}
}

//Dial to address
func dialAddr(addr string) (*rpc.Client,error){

	service, err := rpc.Dial("tcp", addr)
	return service, err
}

//Get next serverentity in the chain, if next index out of bound then get client info at 0
func getNextNodeInfo(rpcc RPCChain) ServerEntity{
	
	pos:=rpcc.CurrrentPosition+1
	if(pos == len(rpcc.Chain)){
		pos=0
	}

	return rpcc.Chain[pos]
}

//construct rpc call to next server
func rpcCall(callType string, rpcc RPCChain) string{
	
	var kvVal ValReply
	var serviceMethod string

	nextNode := getNextNodeInfo(rpcc)
	addr := nextNode.Connection_info
	service := nextNode.Service_info
	storeFunc := nextNode.Entry.Store
	retrieveFunc := nextNode.Entry.Retrieve
	listFunc := nextNode.Entry.List


	dialservice,err:=dialAddr(addr)	

	if (err == nil){
	
		switch callType {
			case "store":
				serviceMethod = service + "." + storeFunc

				dialservice.Call(serviceMethod, rpcc, &kvVal)
				//checkError(err)
				
			case "retrieve":
				serviceMethod = service + "." + retrieveFunc

				dialservice.Call(serviceMethod, rpcc, &kvVal)
				//checkError(err)

			case "list":
				serviceMethod = service + "." + listFunc

				dialservice.Call(serviceMethod, rpcc, &kvVal)
				//checkError(err)
		}
		return kvVal.Val
	}else{
		return "error"
	}
}


//func returns a list of all the files in that directory StorageFiles which has all the storage files
//*assumption that the file is being run from the parent directory  
func listFilesGet() string{

	var fileList []string

	files, _ := ioutil.ReadDir(FileBasePath)  // "./StorageFiles")
	for _, f := range files {
		fileList = append(fileList, f.Name())
	}

	for i := range fileList{
		fmt.Println(fileList[i])     //prints out each file name on seperate line
	}

	//return fileList

	//convert to a single string before returning
	singleString := strings.Join(fileList, ",")
	return singleString


}
