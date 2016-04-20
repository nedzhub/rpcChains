// Usage: go run auth.go [auth ip:port] [frontend ip:port]
//
// - [auth ip:port] : the ip and TCP port on which this authentication server is listening for metadata server connections         
// - [file-store-A ip:port] : the ip and TCP port on file storage server A is listening for auth connections. 
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
	"github.com/arcaneiceman/GoVector/govec"
)

//======================================= SERVICE =======================================
type AuthService int

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
const NodeType int = 2
const NodeService string = "AuthService"
const StoreEntryFunction string = "AStore"
const RetrieveEntryFunction string = ""
const ListEntryFunction string = ""

var CredentialMap map[string]string
var ValidationMap map[string]string

var Logger *govec.GoLog

//=========================================== LOGGER ============================================
func HandleLog(rpcc *RPCChain) {

	prevServer := (rpcc.Chain[rpcc.CurrrentPosition].Service_info)
	logFromPrev := rpcc.Log.Log
	msgFromPrev := new(Msg)
	Logger.UnpackReceive("Store request from " + prevServer, logFromPrev[0:], &msgFromPrev)
	fmt.Println(msgFromPrev.String())
	return

}

func rpcCallLog(callType string, rpcc RPCChain, service string) []byte {

	var outBuf []byte

	if callType == "store" {
		outGoingMessage := Msg{"Hello from auth", time.Now().String()}
		outBuf = Logger.PrepareSend("Store request to " + service, outGoingMessage)
	}

	if callType == "retrieve" {
		outGoingMessage := Msg{"Hello from auth", time.Now().String()}
		outBuf = Logger.PrepareSend("Retrieve request to " + service, outGoingMessage)
	}

	if callType == "list" {
		outGoingMessage := Msg{"Hello from auth", time.Now().String()}
		outBuf = Logger.PrepareSend("List request to " + service, outGoingMessage)
	}

	return outBuf

}

//======================================= SERVICE METHODS =======================================
func (as *AuthService) AStore(rpcc *RPCChain, reply *ValReply) error {

	HandleLog(rpcc)

	updatedPos:=incCurrentPos(*rpcc)

	if secret, ok := CredentialMap[updatedPos.Args.File_Name]; ok {
		//Check secret match, store in validation map to confirm overwrite
		if(secret == updatedPos.Args.Secret_info){
			reply.Val = "Data sent from Auth"
			fmt.Println("STORE RPCC:")
			ValidationMap[updatedPos.Args.File_Name]=updatedPos.Args.Secret_info
			rpcCall("store",updatedPos)
			fmt.Println(updatedPos)
		}else{
			reply.Val = "Data failed to be sent at Auth: error authentication failed"
		}
    } else {
    	//Refuse store if file name in validation map 
    	if _, ok := ValidationMap[updatedPos.Args.File_Name]; ok {
			reply.Val = "Data failed to be sent at Auth: error file name unavailable"
		}else{
			ValidationMap[updatedPos.Args.File_Name]=updatedPos.Args.Secret_info
			rpcCall("store",updatedPos)
			fmt.Println(updatedPos)
		}
    } 
		
	fmt.Println()
	return nil
}

func (as *AuthService) StoreValidation(key *ValReply, reply *ValReply) error {

	if v, ok := ValidationMap[key.Val]; ok {
		CredentialMap[key.Val]=v
		delete(ValidationMap,key.Val)
		reply.Val="Moved to storage map AUTH"
    	return nil
    }else{
    	reply.Val="Not in validation map AUTH"
		return nil
	}
}

func (as *AuthService) UpdateConsistency(arg *CacheContent, reply *ValReply) error {
	reply.Val = "Replica updated"
	CredentialMap=arg.Maps[0]
	return nil
}

func (as *AuthService) AuthRecovery(arg *CacheContent, reply *ValReply) error {
	reply.Val = "Auth data stored"

	if(len(CredentialMap)==0){
		CredentialMap=arg.Maps[0]
	}

	return nil
}

//======================================= MAIN =======================================

func main() {

	Logger = govec.Initialize("auth", "auth")

	// parse argsx`
	usage := fmt.Sprintf("Usage: %s ip:port\n", os.Args[0])
	if len(os.Args) != 3 {
		fmt.Printf(usage)
		os.Exit(1)
	}

	authAddr := os.Args[1]
	frontendAddr := os.Args[2]
	fmt.Println("authAddr:", authAddr," frontendAddr:",frontendAddr)

	CredentialMap = make(map[string]string)
	ValidationMap = make(map[string]string)

	go initListener(authAddr)
	go printMaps()

	//serviceFE, err := rpc.Dial("tcp", frontendAddr)
	//checkError(err)
	//myID := "1"//HandShake(serviceFE)

	//counter:=0
	for {
		//counter++
		//fmt.Println(counter)
		//UpdateToFrontEnd(myID, NodeType, NodeService, authAddr, serviceFE)
		backupAuth(frontendAddr)
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
	service := new(AuthService)
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
		fmt.Println("CredentialMap:",CredentialMap)
		fmt.Println("ValidationMap:",ValidationMap)
		fmt.Println()
		time.Sleep(3000 * time.Millisecond)
	}
}

// remove temporary storage in validation map when exceeding exepected time of 60sec
func mapGarbageCollector(){
	for{
		ValidationMap = make(map[string]string)
		time.Sleep(60000*time.Millisecond)
	}
}

// handshake to get my id key in front end map
func HandShake(dialservice *rpc.Client) string{

	outGoingMessage  := Msg{"Hello frontend from auth", time.Now().String()}
	outGoingBuf := Logger.PrepareSend("Making connection to frontend", outGoingMessage)
	outGoingLog := LogArg{outGoingBuf}

	args:= ValReply{ 
		Val:"hello",
		Log: outGoingLog,
	}

	var kvVal ValReply

	dialservice.Call("FrontEndServiceAuth.Handshake", args, &kvVal)
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
	list[0] = CredentialMap

	node := NodeInfoCache {
		Id: ID,
		Type: typeArg,
		Addr: address, 
		Service: service,
		Entry: entryFunc,
		Maps: list,
	}

	var kvVal ValReply

	err := dialservice.Call("FrontEndServiceAuth.ReportServerActivity", node, &kvVal)
	checkError(err)
	//fmt.Println("ReportServerActivity err:",err)
	//fmt.Println("Updated activity status:", node, kvVal.Val)
	
}

//Set current position to +=1
func incCurrentPos(rpcc RPCChain) RPCChain{
	rpcc.CurrrentPosition++
	return rpcc
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

	rpcc.Log.Log = rpcCallLog(callType, rpcc, service)

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
	//fmt.Println(service ,"STORE reply:", kvVal.Val)
	return kvVal.Val
}

func backupAuth(frontendAddr string) error{
	maps := make([]map[string]string,1)
	maps[0]=CredentialMap
	cache := CacheContent{Maps:maps,}

	dialservice, err:=dialAddr(frontendAddr)
	if (err == nil){
		var kvVal ValReply
		serviceMethod := "FrontEndMapService" + "." + "AuthRecovery"
		dialservice.Call(serviceMethod, cache, &kvVal)
		//checkError(err)
	}
	
	return nil
}