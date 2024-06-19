package kvsrv

import (
	"log"
	"sync"
)

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type KVServer struct {
	mu sync.Mutex

	data map[string]string
	// Your definitions here.
	request map[int64]string
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()
	reply.Value = kv.data[args.Key]
}

func (kv *KVServer) Put(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if _, ok := kv.request[args.Id]; ok {
		reply.Value = kv.request[args.Id]
		return
	}
	kv.data[args.Key] = args.Value
	kv.request[args.Id] = args.Value
	reply.Value = args.Value
}

func (kv *KVServer) Append(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if _, ok := kv.request[args.Id]; ok {
		reply.Value = kv.request[args.Id]
		return
	}
	oldValue := kv.data[args.Key]
	kv.data[args.Key] = oldValue + args.Value
	kv.request[args.Id] = oldValue
	reply.Value = kv.request[args.Id]
}

func (kv *KVServer) Finish(args *PutAppendArgs, reply *PutAppendReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	delete(kv.request, args.Id)
}

func StartKVServer() *KVServer {
	kv := new(KVServer)
	kv.data = make(map[string]string)
	// You may need initialization code here.
	kv.request = make(map[int64]string)
	return kv
}
