// Usage: go run filestoreA.go [file-store-A ip:port] [frontend ip:port] r
//
// - [file-store-A ip:port] : the ip and TCP port on which this client is listening for file server connections         
// - [client ip:port] : the ip and TCP port on which client is listening for client connections. 
// - r : replication factor
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
	"bufio"
	"io"
	"github.com/arcaneiceman/GoVector/govec"
)


//======================================= SERVICE =======================================
type FilestoreServiceA int

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
const NodeType int = 3
const NodeService string = "FilestoreServiceA"
const StoreEntryFunction string = "FAStore"
const RetrieveEntryFunction string = "FARetrieve"
const ListEntryFunction string = "FAList"

const MetadataService string = "MetadataService"
const AuthService string = "AuthService"


//======================================= VARIABLES =======================================
const FileBasePath = "./StorageFilesA/"

var FileContentMapA map[string]string
var CredentialMap map[string]string

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
func (fsa *FilestoreServiceA) FAStore(rpcc *RPCChain, reply *ValReply) error {

	var callType = "store"
	HandleLog(callType, rpcc)

	reply.Val = "Data sent from FileStore A STORE"
	fmt.Println("STORE RPCC:")
	updatedPos:=incCurrentPos(*rpcc)
	dbEntity:=containsEntity(updatedPos.Chain,MetadataService)
	auEntity:=containsEntity(updatedPos.Chain,AuthService)

	if(dbEntity!=ServerEntity{} && auEntity!=ServerEntity{}){
		dbConn,dbErr:=dialAddr(dbEntity.Connection_info)
		auConn,auErr:=dialAddr(auEntity.Connection_info)
		if(dbErr==nil && auErr==nil){
			nilFlag:=rpcCall("store",updatedPos)
			if(nilFlag != "nil"){
				arg := ValReply {
					Val: updatedPos.Args.File_Name,
				}
				var kvVal ValReply
				var kvVal2 ValReply
				serviceMethod := dbEntity.Service_info + "." + "StoreValidation"
				err := dbConn.Call(serviceMethod, arg, &kvVal)
				fmt.Println(kvVal.Val)
				checkError(err)
				
				serviceMethod = auEntity.Service_info + "." + "StoreValidation"
				err = auConn.Call(serviceMethod, arg, &kvVal2)
				checkError(err)
				fmt.Println(kvVal.Val)

				FileContentMapA[updatedPos.Args.File_Name]=updatedPos.Args.Text_content
			}
		}	
	}

	fmt.Println(updatedPos)
	return nil
}
func (fsa *FilestoreServiceA) FARetrieve(rpcc *RPCChain, reply *ValReply) error {

	var callType = "retrieve"
	HandleLog(callType, rpcc)

	reply.Val = "Data sent from FileStore A RETRIEVE"
	fmt.Println("RETRIEVE RPCC:")
	updatedPos:=incCurrentPos(*rpcc)

	if _, ok := FileContentMapA[updatedPos.Args.File_Name]; ok {
		updatedPos.Args.Text_content=FileContentMapA[updatedPos.Args.File_Name]
    } else {
    	updatedPos.Args.File_Name=""
    	updatedPos.Args.Text_content=""
    } 
	rpcCall("retrieve",updatedPos)
	

	fmt.Println(updatedPos)
	return nil
}

func (fsa *FilestoreServiceA) FAList(rpcc *RPCChain, reply *ValReply) error {

	var callType = "list"
	HandleLog(callType, rpcc)

	reply.Val = "Data sent from FileStore A LIST"
	fmt.Println("LIST RPCC:")
	updatedPos:=incCurrentPos(*rpcc)

	// //updatedPos.Args.File_Name = "A name1 name2 name3"
	// lfg := listFilesGet() // array of strings format   //no longer using this method 
	// fmt.Println("FILE STORE A'S LIST STRING IS : " , lfg)
 	//    //singleString := strings.Join(lfg, ",") 	  // ex: "string1.txt,string2.txt,string3.txt
	// updatedPos.Args.File_Name = lfg
	updatedPos.Args.File_Name = "ChangingToMapFilesInMetaA"


	rpcCall("list",updatedPos)
	fmt.Println(updatedPos)

	return nil
}

func (fsa *FilestoreServiceA) UpdateConsistency(arg *CacheContent, reply *ValReply) error {
	reply.Val = "Replica updated"
	if (len(arg.Maps) ==1){
		FileContentMapA=arg.Maps[0]
	}
	return nil
}

func (fsa *FilestoreServiceA) AuthRecovery(arg *CacheContent, reply *ValReply) error {
	reply.Val = "Auth data stored"
	CredentialMap = arg.Maps[0]
	return nil
}

//======================================= MAIN =======================================

func main() {

	Logger = govec.Initialize("filestoreA", "filestoreA")

	// parse args
	usage := fmt.Sprintf("Usage: %s ip:port\n", os.Args[0])
	if len(os.Args) != 5 {
		fmt.Printf(usage)
		os.Exit(1)
	}

	filestoreAddr := os.Args[1]
	frontendAddr := os.Args[2]
	authAddr := os.Args[3]
	replicationFactor := os.Args[4]
	fmt.Println("filestoreAddrB:", filestoreAddr, " frontendAddr:",frontendAddr, "authAddr:",authAddr, " replicationFactor:", replicationFactor)

	FileContentMapA = make(map[string]string)
	CredentialMap = make(map[string]string)

	go initListener(filestoreAddr)
	go printMaps()
	
	//this is how you get the list of all files file storage has in its folder FileStorage
	// lfg := listFilesGet()
	// if len(lfg) == 0 {
	// 	fmt.Println("No Files Detected")
	// }
	//convert the list of strings to a single comma seperated string
	// singleString := strings.Join(lfg, ",") 	  // ex: "string1.txt,string2.txt,string3.txt"
	// fmt.Println(singleString);   

	//x := 
	// getFile("storageFile2.txt")
	//fmt.Println(x)

	//counter:=0

	serviceFE, err := rpc.Dial("tcp", frontendAddr)
	checkError(err)
	myID := HandShake(serviceFE)
	UpdateToFrontEnd(myID, NodeType, NodeService, filestoreAddr, serviceFE)
	//counter:=0
	for {
		//counter++
		//fmt.Println(counter)
		//UpdateToFrontEnd(myID, NodeType, NodeService, metadataAddr, serviceFE)
		//time.Sleep(1000*time.Millisecond)

		serviceFE, err = rpc.Dial("tcp", frontendAddr)
		if err == nil {
			UpdateToFrontEnd(myID, NodeType, NodeService, filestoreAddr, serviceFE)
		}

		if(len(CredentialMap) != 0){
			backupAuth(authAddr)	
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
	service := new(FilestoreServiceA)
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
		fmt.Println("CredentialMap:",CredentialMap)
		fmt.Println("Printing stored files...")
		fileCount:=1	
		for k,v :=  range FileContentMapA{
			fmt.Println(fileCount,"FILENAME:",k)
			fmt.Println("  CONTENT:",v)
			fileCount++
		}
		time.Sleep(3000 * time.Millisecond)
	}
}

// handshake to get my id key in front end map
func HandShake(dialservice *rpc.Client) string{

	outGoingHandshake := Msg{"Hello frontend from FSA", time.Now().String()}
	outGoingBuf := Logger.PrepareSend("Making connection to frontend", outGoingHandshake)
	outGoingLog := LogArg{outGoingBuf}

	args:= ValReply {
		Val:"hello",
		Log: outGoingLog,
	}
	var kvVal ValReply

	dialservice.Call("FrontEndServiceFilestoreA.Handshake", args, &kvVal)
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
	list[0] = FileContentMapA

	node := NodeInfoCache {
		Id: ID,
		Type: typeArg,
		Addr: address, 
		Service: service,
		Entry: entryFunc,
		Maps: list,
	}

	var kvVal ValReply

	dialservice.Call("FrontEndServiceFilestoreA.ReportServerActivity", node, &kvVal)
	//checkError(err)
	//fmt.Println("ReportServerActivity err:",err)
	//fmt.Println("Updated activity status:", node, kvVal.Val)
	
}

func backupAuth(authAddr string) error{
	maps :=make([]map[string]string,1)
	maps[0]=CredentialMap
	cache := CacheContent{Maps:maps,}

	dialservice, err:=dialAddr(authAddr)
	if (err == nil){
		var kvVal ValReply
		serviceMethod := "AuthService" + "." + "AuthRecovery"
		dialservice.Call(serviceMethod, cache, &kvVal)
		//checkError(err)
	}
	
	return nil
}


//Set current position to +=1
func incCurrentPos(rpcc RPCChain) RPCChain{
	rpcc.CurrrentPosition++
	return rpcc
}

//Test Dial
func testDial(addr string) bool {
	_, err := rpc.Dial("tcp", addr)
	if err != nil {
		return false
	}else{
		return true
	}	
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
func dialAddr(addr string) (*rpc.Client,error) {

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

	if(err == nil){
		switch callType {
			case "store":
				serviceMethod = service + "." + storeFunc
				dialservice.Call(serviceMethod, rpcc, &kvVal)
				// checkError(err)
			case "retrieve":
				serviceMethod = service + "." + retrieveFunc
				dialservice.Call(serviceMethod, rpcc, &kvVal)
				// checkError(err)
			case "list":
				serviceMethod = service + "." + listFunc
				dialservice.Call(serviceMethod, rpcc, &kvVal)
				// checkError(err)
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

	files, _ := ioutil.ReadDir(FileBasePath)//"./StorageFiles")
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


//reads the given filename (IO) and returns it in byte array format 
func getFile(fname string) []byte{

	filename := strings.TrimSpace(fname)
	if filename == "" {
		fmt.Println("File name is empty")
		//os.Exit(1)
	}	

	// Read the file
	fmt.Println(FileBasePath+filename)
	f, err:= os.Open(FileBasePath+filename)
	checkError(err)

	fileReader := bufio.NewReader(f)

	fileBuf := make([]byte, 1024)
	for {
		n, err := fileReader.Read(fileBuf)
		if err != nil && err != io.EOF {
			panic(err)
		}
		if n == 0 {
			break
		}

	}

	sizeOfFile := len(fileBuf)
	fmt.Println("size is: " , sizeOfFile)

	return fileBuf

}