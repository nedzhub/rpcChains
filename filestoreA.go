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
    "./rpcc"
    "os"
    "time"
    "net"
    "log"
    //"strconv"
    "io/ioutil"
    "strings"
    "bufio"
    "io"
    "encoding/gob"
)


//======================================= SERVICE =======================================
type FilestoreServiceA int

//======================================= STRUCTS =======================================
type ValArgs struct{
	File_Name string
	Text_content string
	Secret_info string
    ErrorCode int
}

type ValReply struct {
	Val string // value; depends on the call
}

type ValMetadata struct {
    FilestoreMapA map[int]NodeInfo
    FilestoreMapB map[int]NodeInfo
}

// Type 0 is metadata server, 1 is auth server, 2 is file storage A, 3 is file storage B
type NodeInfoCache struct {
	Id int
	Type int
	Addr string
	Service string
	Maps []map[string]string
}

type NodeInfo struct {
	Id int
	Type int
	Addr string
	Service string
}

type CacheContent struct{
	Maps []map[string]string
}
//======================================= VARIABLES =======================================
const NodeType int = 3
const NodeService string = "FilestoreServiceA"
const StoreEntryFunction string = "FAStore"
const RetrieveEntryFunction string = "FARetrieve"
const ListEntryFunction string = "FAList"

const (
    INCOMPLETE_CHAIN = iota
    SUCCESSFUL_COMPLETED = iota
    INVALID_AUTH_ERROR = iota
)

const MetadataService string = "MetadataService"
const AuthService string = "AuthService"


//======================================= VARIABLES =======================================
const FileBasePath = "./StorageFilesA/"

var FileContentMapA map[string]string
var CredentialMap map[string]string

//======================================= SERVICE METHODS =======================================
func (fsa *FilestoreServiceA) FAStore(chain rpcc.RPCChain, reply *bool) error {
	fmt.Println("STORE RPCC:")
    
	dbEntity := chain.FindEntity(MetadataService)
	auEntity := chain.FindEntity(AuthService)

	if(dbEntity != nil && auEntity != nil){
        dbConn,dbErr := rpcc.Dial(*dbEntity)
		auConn,auErr := rpcc.Dial(*auEntity)
        
		if (dbErr==nil && auErr==nil) {
            args := chain.FirstEntity().Args.(ValArgs)
            args.ErrorCode = SUCCESSFUL_COMPLETED
            chain.FirstEntity().Args = args
            
            err := chain.CallIndex(1, 10000)
            
			if(err == nil) {
				arg := ValReply {
					Val: args.File_Name,
				}
                
				var kvVal ValReply
				var kvVal2 ValReply
				serviceMethod := dbEntity.Service_info + "." + "StoreValidation"
				err := dbConn.Call(serviceMethod, arg, &kvVal)
				fmt.Println(kvVal.Val)
				checkError(err)
                dbConn.Close()
                
				serviceMethod = auEntity.Service_info + "." + "StoreValidation"
				err = auConn.Call(serviceMethod, arg, &kvVal2)
				checkError(err)
				fmt.Println(kvVal.Val)
                auConn.Close()
                
				FileContentMapA[args.File_Name]=args.Text_content
			}
		}	
	}

	fmt.Println(chain)
    
    *reply = true
	return nil
}
func (fsa *FilestoreServiceA) FARetrieve(chain rpcc.RPCChain, reply *bool) error {
	fmt.Println("RETRIEVE RPCC:")
    
    args := chain.FirstEntity().Args.(ValArgs)
    
    // Populate the return value with the content
	if _, ok := FileContentMapA[args.File_Name]; ok {
		args.Text_content=FileContentMapA[args.File_Name]
    } else {
    	args.Text_content=""
    }
    
    args.ErrorCode = SUCCESSFUL_COMPLETED
    
    // Update the args and call frontend
    chain.FirstEntity().Args = args
	chain.CallIndex(1, 10000)
	
	fmt.Println(chain)
    *reply = true;
	return nil
}

func (fsa *FilestoreServiceA) FAList(chain *rpcc.RPCChain, reply *bool) error {
	fmt.Println("LIST RPCC:")
	//updatedPos.Args.File_Name = "A name1 name2 name3"
	lfg := listFilesGet() // array of strings format
	fmt.Println("FILE STORE A'S LIST STRING IS : " , lfg)

    //singleString := strings.Join(lfg, ",") 	  // ex: "string1.txt,string2.txt,string3.txt
    args := chain.FirstEntity().Args.(ValArgs)
    if (args.File_Name == "") {
        args.File_Name = lfg
    } else {
        args.File_Name = args.File_Name + "," + lfg
    }
    chain.FirstEntity().Args = args

	chain.CallNext(10000)
	fmt.Println(chain)

    *reply = true
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
	// parse args
	usage := fmt.Sprintf("Usage: %s ip:port\n", os.Args[0])
	if len(os.Args) != 5 {
		fmt.Printf(usage)
		os.Exit(1)
	}
    
    gob.Register(ValArgs{})
    gob.Register(ValMetadata{})
    gob.Register(NodeInfo{})

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
	UpdateToFrontEnd(NodeType, NodeService, filestoreAddr, serviceFE)
	//counter:=0
	for {
		//counter++
		//fmt.Println(counter)
		//UpdateToFrontEnd(myID, NodeType, NodeService, metadataAddr, serviceFE)
		//time.Sleep(1000*time.Millisecond)

		serviceFE, err = rpc.Dial("tcp", frontendAddr)
		if err == nil {
			UpdateToFrontEnd(NodeType, NodeService, filestoreAddr, serviceFE)
		}

		if(len(CredentialMap) != 0){
			backupAuth(authAddr)	
		}

		time.Sleep(1000 * time.Millisecond)
	}

}

//======================================= HELPER FUNCTIONS =======================================

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

// send this node's information to front end 
func UpdateToFrontEnd(typeArg int, service string, address string, dialservice *rpc.Client){
	list := make([]map[string]string,1)
	list[0] = FileContentMapA

	node := NodeInfoCache {
		Type: typeArg,
		Addr: address, 
		Service: service,
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

//Test Dial
func testDial(addr string) bool {
	_, err := rpc.Dial("tcp", addr)
	if err != nil {
		return false
	}else{
		return true
	}	
}
//Dial to address
func dialAddr(addr string) (*rpc.Client,error) {

	service, err := rpc.Dial("tcp", addr)
	return service, err
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