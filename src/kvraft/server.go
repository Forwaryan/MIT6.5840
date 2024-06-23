package kvraft

import (
	"log"
	"sync"
	"sync/atomic"
	"time"

	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raft"
)

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

const ExecuteTimeout = 500 * time.Millisecond

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	ClientId  int64
	RequestId int64
	OpType    string
	Key       string
	Value     string
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	KVDB           map[string]string // 状态机，记录(K,V)键值对
	waitChMap      map[int]chan *Op  //通知chan，key为日志的下标，值为通道
	lastRequestMap map[int64]int64   // 保存每个客户端对应的最近的一次请求ID
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	op := Op{
		ClientId:  args.ClientId,
		RequestId: args.RequestId,
		OpType:    "Get",
		Key:       args.Key,
	}
	//该命令提交时的索引  当前的任期
	index, term, isLeader := kv.rf.Start(op)
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}
	kv.mu.Lock()
	id := kv.me
	DPrintf("[%d] send Get to leader, log index [%d], log term [%d], args [%v]", id, index, term, args)
	waitChan, exist := kv.waitChMap[index]
	if !exist {
		kv.waitChMap[index] = make(chan *Op, 1)
		waitChan = kv.waitChMap[index]
	}
	DPrintf("[%d] wait for timeout", id)
	kv.mu.Unlock()
	select {
	case res := <-waitChan:
		DPrintf("[%d] receive res from waitChan [%v]", id, res)
		reply.Value = res.Value
		reply.Err = OK
		currentTerm, stillLeader := kv.rf.GetState()
		if !stillLeader || term != currentTerm {
			DPrintf("[%d] has accident, stillLeader [%t], term [%d], currentTerm [%d]", id, stillLeader, term, currentTerm)
			reply.Err = ErrWrongLeader
		}
	case <-time.After(ExecuteTimeout):
		DPrintf("[%d] timeout", id)
		reply.Err = ErrWrongLeader
	}
	kv.mu.Lock()
	delete(kv.waitChMap, index)
	kv.mu.Unlock()
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	kv.mu.Lock()
	id := kv.me
	if kv.isInvalidRequest(args.ClientId, args.RequestId) {
		DPrintf("[%d] receive out of data request", id)
		//请求不合法会被忽略
		reply.Err = OK
		kv.mu.Unlock()
		return
	}
	kv.mu.Unlock()

	op := Op{
		ClientId:  args.ClientId,
		RequestId: args.RequestId,
		OpType:    args.Op,
		Key:       args.Key,
		Value:     args.Value,
	}

	DPrintf("innnnnnnnnnn ----------[%v]", op)

	index, term, isLeader := kv.rf.Start(op)
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}
	kv.mu.Lock()
	DPrintf("[%d] send PutAppend to leader, log index [%d], log term [%d], args [%v]", id, index, term, args)
	//给对应日志的索引创建chan
	waitChan, exitst := kv.waitChMap[index]
	if !exitst {
		kv.waitChMap[index] = make(chan *Op, 1)
		waitChan = kv.waitChMap[index]
	}
	DPrintf("[%d] wait for timeout", id)
	kv.mu.Unlock()

	select {
	case res := <-waitChan:
		DPrintf("[%d] receive res from waitChan [%v]", id, res)
		reply.Err = OK
		currentTerm, stillLeader := kv.rf.GetState()
		if !stillLeader || currentTerm != term {
			DPrintf("[%d] has accident, stillLeader [%t], term [%d], currentTerm [%d]", id, stillLeader, term, currentTerm)
			reply.Err = ErrWrongLeader
		}
	case <-time.After(ExecuteTimeout):
		DPrintf("[%d] timeout!", id)
		reply.Err = ErrWrongLeader
	}

	kv.mu.Lock()
	delete(kv.waitChMap, index)
	kv.mu.Unlock()
}

// 检查当前的命令是否有效，非Get命令需要检查 保持幂等性
func (kv *KVServer) isInvalidRequest(ClientId int64, requestId int64) bool {
	if lastRequestId, ok := kv.lastRequestMap[ClientId]; ok {
		//请求以过期
		if requestId <= lastRequestId {
			return true
		}
	}
	return false
}

// func (kv *KVServer) Put(args *PutAppendArgs, reply *PutAppendReply) {
// 	// Your code here.
// }

// func (kv *KVServer) Append(args *PutAppendArgs, reply *PutAppendReply) {
// 	// Your code here.
// }

// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

// 监听Raft提交的apllyMsg，根据其类别执行不同的操作
// 为命令时必须执行，执行完后检查是否需要给waitChan发消息
func (kv *KVServer) applier() {
	for !kv.killed() {
		//raft中有提交的命令/快照快速通知KVserver
		applyMsg := <-kv.applyCh
		kv.mu.Lock()
		DPrintf("[%d] receives applyMsg [%v]", kv.me, applyMsg)
		//根据接收到的是命令还是快照来决定相应的操作，3A只执行命令
		if applyMsg.CommandValid {
			op := applyMsg.Command.(Op)
			kv.execute(&op)
			currentTermm, isLeader := kv.rf.GetState()
			// 若当前服务器不再是leader或者发生分区的旧leader，不需要通知回复客户端
			// clerk在一个任期内向kvsever领导者发送请求、等待答复超时并在另一个任期内将请求重新发送给领导者
			//对应操作op完成后给KVserver回应，然后给客户端回应
			if isLeader && applyMsg.CommandTerm == currentTermm {
				kv.notifyWaitCh(applyMsg.CommandIndex, &op)
			}
		} else if applyMsg.SnapshotValid {
			//

		}
		kv.mu.Unlock()
	}
}

// 给waitCh发送通知，让其生成响应
// 在发送之前需要检查waitCh是否关闭
func (kv *KVServer) notifyWaitCh(index int, op *Op) {
	DPrintf("[%d] notifyWaitCh [%d]", kv.me, index)
	if waitCh, ok := kv.waitChMap[index]; ok {
		waitCh <- op
	}
}

// 执行命令，若为重复命令且不是Get 则忽略 保持幂等性
// 否则根据OpType 执行命令并更新到该客户端最近一次请求的Id
func (kv *KVServer) execute(op *Op) {
	DPrintf("[%d] apply command [%v] success", kv.me, op)
	//执行完命令后才会更新，于是可能发生重复命令第一次检查时不是，然后被执行了两遍
	//所以在真正执行命令前还需要再检查一遍，若发现有重复日志且不是Get，直接忽略
	if op.OpType != "Get" && kv.isInvalidRequest(op.ClientId, op.RequestId) {
		return
	} else {
		switch op.OpType {
		case "Get":
			op.Value = kv.KVDB[op.Key]
		case "Put":
			kv.KVDB[op.Key] = op.Value
		case "Append":
			str := kv.KVDB[op.Key]
			kv.KVDB[op.Key] = str + op.Value
		}
		kv.UpdateLastRequest(op)
	}
}

// 更新对客户端对应的最近一次请求的Id，避免执行过期的RPC或者已经执行过的命令
func (kv *KVServer) UpdateLastRequest(op *Op) {
	lastRequestId, ok := kv.lastRequestMap[op.ClientId]
	if (ok && lastRequestId < op.RequestId) || !ok {
		kv.lastRequestMap[op.ClientId] = op.RequestId
	}
}

// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	// You may need initialization code here.

	kv.KVDB = make(map[string]string)
	kv.waitChMap = make(map[int]chan *Op)
	kv.lastRequestMap = make(map[int64]int64)

	go kv.applier()
	return kv
}
