package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	//	"bytes"
	"math"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	//	"6.5840/labgob"
	"6.5840/labrpc"
)

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 3D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 3D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

const HEATBEAT float64 = 150   // leader send heatbit per 150ms
const TIMEOUTLOW float64 = 500 // the timeout period randomize between 500ms - 1000ms
const TIMEOUTHIGH float64 = 1000
const CHECKPERIOD float64 = 300       // check timeout per 300ms
const CHECKAPPLYPERIOD float64 = 10   // check apply per 10ms
const CHECKAPPENDPERIOED float64 = 10 // check append per 10ms
const CHECKCOMMITPERIOED float64 = 10 // check commit per 10ms
const FOLLOWER int = 0
const CANDIDATE int = 1
const LEADER int = 2

type LogEntry struct {
	Term    int
	Command interface{}
}

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (3A, 3B, 3C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	//persistent state
	currentTerm int //最新的term
	votedFor    int //投票给谁
	log         []LogEntry

	// volatile state on all server
	commitIndex int //已提交的最高日志条目的索引
	lastApplied int //最高日志条目的索引
	timestamp   time.Time
	state       int //追随者，候选者，领导者
	cond        *sync.Cond
	applyCh     chan ApplyMsg
	// volatile state on leaders
	nextIndex  []int //
	matchIndex []int
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int = rf.currentTerm
	var isleader bool = (rf.state == LEADER)
	// Your code here (3A).
	return term, isleader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).
func (rf *Raft) persist() {
	// Your code here (3C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// raftstate := w.Bytes()
	// rf.persister.Save(raftstate, nil)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (3C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (3D).

}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (3A, 3B).
	Term         int //候选者的term
	CandidateId  int
	LastLogIndex int //最新的日志index
	LastLogTerm  int //最新的term
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (3A).
	Term        int  //当前的term
	VoteGranted bool //候选人是否获得了该票
}

// example RequestVote RPC handler.

func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (3A, 3B).
	// 现在的rf因为调用了peer[server]变换为了被请求者，args.CandidateId是候选者
	rf.mu.Lock()
	defer rf.mu.Unlock()

	DPrintf("[term %d]: Raft[%d] receive requestVote from Raft[%d]", rf.currentTerm, rf.me, args.CandidateId)

	//requestvote rpc的第一种情况，候选者任期小于当前server任期
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false

		return
	}

	//候选者有着比server更高的任期，修改server的信息和状态
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		//如果是候选者 -> FOLLOWER
		rf.state = FOLLOWER
		//投票给自己的票要重新投
		rf.votedFor = -1
	}

	reply.Term = rf.currentTerm
	//后半段|| rf.votedFor == args.CandidateId 应该是无用的
	if rf.votedFor == -1 || rf.votedFor == args.CandidateId {
		//server的最后一个term
		lastlogterm := rf.log[len(rf.log)-1].Term
		//检查候选者的日志是否和server的一样新，候选者的日志至少要和server一样新才能授予投票
		if args.LastLogTerm > lastlogterm || (args.LastLogTerm == lastlogterm && args.LastLogIndex >= len(rf.log)-1) {

			rf.timestamp = time.Now()
			rf.votedFor = args.CandidateId
			reply.VoteGranted = true
			DPrintf("[term %d]: Raft [%d] vote for Raft [%d]", rf.currentTerm, rf.me, rf.votedFor)
			return
		}
	}

	reply.VoteGranted = false

}

// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.

// 使用 Raft 的服务（例如一个键/值服务器）希望开始对下一个命令进行一致性协议，
// 将其追加到 Raft 的日志中。如果这个服务器不是领导者，则返回 false。
// 否则，开始一致性协议并立即返回。不能保证这个命令最终会被提交到 Raft 日志中，
// 因为领导者可能会失败或失去选举。即使 Raft 实例已经被杀死，
// 这个函数也应该优雅地返回。
//
// 第一个返回值是如果命令被提交，它将出现在日志中的索引。
// 第二个返回值是当前的任期（term）。
// 第三个返回值是如果这个服务器认为自己是领导者，则返回 true。

func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (3B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.state != LEADER || rf.killed() {
		return index, term, false
	}

	DPrintf("[term %d]: Raft [%d] start consensus", rf.currentTerm, rf.me)

	index = len(rf.log)
	term = rf.currentTerm
	rf.log = append(rf.log, LogEntry{Term: rf.currentTerm, Command: command})

	return index, term, isLeader
}

// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

type AppendEntriesArgs struct {
	Term         int //leader的term
	LeaderId     int
	PrevLogIndex int        //最新条目的前一个index
	PrevLogTerm  int        //最新term的前一个term
	Entries      []LogEntry //要存储的日志条目，对于心跳可能会发送多个
	LeaderCommit int        //领导者commit的index
}

type AppendEntriesReply struct {
	Term    int  //最新的term供leader更新
	Success bool //为true时：follwer包含与prev匹配的条目
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {

	// 现在的rf变为server，领导者在args中
	rf.mu.Lock()
	defer rf.mu.Unlock()
	DPrintf("[term %d]: Raft[%d] [state %d] receive AppendEntries from Raft[%d]", rf.currentTerm, rf.me, rf.state, args.LeaderId)

	//如果leader的term小于当前的term reply false
	if args.Term < rf.currentTerm {
		reply.Success = false
	}

	//领导者有更高的term
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.state = FOLLOWER
		rf.votedFor = -1
	}

	reply.Term = rf.currentTerm

	rf.timestamp = time.Now()
	//如果当前节点的日志条目和领导者节点的不匹配，则返回false取消该次追加日志，以免恶化日志不匹配问题
	if args.PrevLogIndex >= len(rf.log) || rf.log[args.PrevLogIndex].Term != args.PrevLogTerm {
		reply.Success = false
		return
	}

	//删除之前矛盾的日志，追加新日志，如果Success为true的情况下
	i := 0
	j := args.PrevLogIndex + 1
	DPrintf("%d", j)
	for i = 0; i < len(args.Entries); i++ {
		if j >= len(rf.log) {
			break
		}

		if rf.log[j].Term == args.Entries[i].Term {
			j++
		} else {
			//将rf从j开始的部分替换为领导者从i开始的部分
			rf.log = append(rf.log[:j], args.Entries[i:]...)
			i = len(args.Entries)
			j = len(rf.log) - 1
			break
		}
	}

	if i < len(args.Entries) {
		rf.log = append(rf.log, args.Entries[i:]...)
		j = len(rf.log) - 1
	} else {
		j--
	}
	// DPrintLog(rf)
	reply.Success = true
	//这里也没有完全看懂
	if args.LeaderCommit > rf.commitIndex {
		//更新commitIndex
		oriCommitIndex := rf.commitIndex
		rf.commitIndex = int(math.Min(float64(args.LeaderCommit), float64(j)))
		DPrintf("[term %d]:Raft [%d] [state %d] commitIndex is %d", rf.currentTerm, rf.me, rf.state, rf.commitIndex)
		if rf.commitIndex > oriCommitIndex {

			rf.cond.Broadcast()
		}
	}

}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)

	return ok
}

// AppendEntries RPC
// 领导者调用来复制日志条目

func (rf *Raft) callAppendEntries(server int, term int, prevLogIndex int, prevLogTerm int, entries []LogEntry, leaderCommit int) bool {
	DPrintf("[term %d]:Raft [%d] [state %d] sends appendentries RPC to server[%d]", rf.currentTerm, rf.me, rf.state, server)
	args := AppendEntriesArgs{
		Term:         term,
		LeaderId:     rf.me,
		PrevLogIndex: prevLogIndex,
		PrevLogTerm:  prevLogTerm,
		Entries:      entries,
		LeaderCommit: leaderCommit,
	}

	reply := AppendEntriesReply{}
	ok := rf.sendAppendEntries(server, &args, &reply)
	if !ok {
		return false
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()

	if term != rf.currentTerm {
		return false
	}

	//更新term
	if reply.Term > rf.currentTerm {
		rf.currentTerm = reply.Term
		rf.state = FOLLOWER
		rf.votedFor = -1
	}

	return reply.Success

}

func (rf *Raft) leaderHeartBeat() {
	DPrintf("[term %d]:Raft [%d] [state %d] becomes leader !", rf.currentTerm, rf.me, rf.state)
	for {
		rf.mu.Lock()
		if rf.killed() || rf.state != LEADER {
			rf.mu.Unlock()
			return
		}

		term := rf.currentTerm
		leaderCommit := rf.commitIndex
		preLogIndex := len(rf.log) - 1
		preLogTerm := rf.log[preLogIndex].Term
		rf.mu.Unlock()

		for server := range rf.peers {
			if server == rf.me {
				continue
			}

			go func(server int) {
				rf.callAppendEntries(server, term, preLogIndex, preLogTerm, make([]LogEntry, 0), leaderCommit)
			}(server)
		}

		time.Sleep(time.Microsecond * time.Duration(HEATBEAT))
	}
}

func (rf *Raft) appendChecker(server int) {
	for {
		rf.mu.Lock()
		if rf.killed() || rf.state != LEADER {
			rf.mu.Unlock()
			return
		}

		lastlogidx := len(rf.log) - 1
		nextidx := rf.nextIndex[server]
		term := rf.currentTerm
		preLogIndex := nextidx - 1
		preLogTerm := rf.log[preLogIndex].Term
		leaderCommit := rf.commitIndex
		entries := rf.log[nextidx:]
		rf.mu.Unlock()
		if lastlogidx >= nextidx {
			DPrintf("[term %d]: Raft[%d] send real appendEntries to Raft[%d]", rf.currentTerm, rf.me, server)
			success := rf.callAppendEntries(server, term, preLogIndex, preLogTerm, entries, leaderCommit)
			rf.mu.Lock()

			if term != rf.currentTerm {
				rf.mu.Unlock()
				continue
			}
			// append entries successfully, update nextIndex and matchIndex
			if success {
				rf.nextIndex[server] = nextidx + len(entries)
				rf.matchIndex[server] = preLogIndex + len(entries)
				DPrintf("[term %d]: Raft[%d] successfully append entries to Raft[%d]", rf.currentTerm, rf.me, server)

			} else {
				// if AppendEntries fails because of log inconsistency: decrement nextIndex and retry
				rf.nextIndex[server] = int(math.Max(1.0, float64(rf.nextIndex[server]-1)))
				rf.mu.Unlock()
				continue
			}
			rf.mu.Lock()
		}

		time.Sleep(time.Millisecond * time.Duration(CHECKAPPENDPERIOED))
	}
}

func (rf *Raft) allocateAppendCheckers() {
	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}
		go rf.appendChecker(i)
	}
}

// 候选者向server发出请求投票
func (rf *Raft) callRequestVote(server int, term int, lastlogidx int, lastlogterm int) bool {
	DPrintf("[term %d]:Raft [%d][state %d] sends requestvote RPC to server[%d]", term, rf.me, rf.state, server)

	//rf.me是候选者  server是另一个服务器
	args := RequestVoteArgs{
		Term:         term,
		CandidateId:  rf.me,
		LastLogIndex: lastlogidx,
		LastLogTerm:  lastlogterm,
	}

	reply := RequestVoteReply{}
	ok := rf.sendRequestVote(server, &args, &reply)
	if !ok {
		return false
	}

	//这段代码好像没用
	// rf.mu.Lock()
	// defer rf.mu.Unlock()

	// if term != rf.currentTerm {
	// 	rf.currentTerm = reply.Term
	// 	rf.state = FOLLOWER
	// 	rf.votedFor = -1
	// }

	return reply.VoteGranted
}

// 开始选举
func (rf *Raft) startElection() {
	rf.mu.Lock()
	rf.currentTerm++
	rf.votedFor = rf.me
	rf.state = CANDIDATE
	rf.timestamp = time.Now()
	term := rf.currentTerm
	lastlogidx := len(rf.log) - 1
	lastlogterm := rf.log[lastlogidx].Term
	rf.mu.Unlock()
	DPrintf("[term %d]:Raft [%d][state %d] starts an election", term, rf.me, rf.state)
	//给别的server发送requestvote rpc
	votes := 1
	electionFinished := false
	var voteMutex sync.Mutex

	for server := range rf.peers {
		if server == rf.me {
			DPrintf("vote for self : Raft[%d]", rf.me)
			continue
		}

		go func(server int) {
			voteGranted := rf.callRequestVote(server, term, lastlogidx, lastlogterm)
			voteMutex.Lock()

			if voteGranted && !electionFinished {
				votes++
				if votes*2 > len(rf.peers) {
					electionFinished = true
					rf.mu.Lock()
					rf.state = LEADER

					for i := 0; i < len(rf.peers); i++ {
						rf.nextIndex[i] = len(rf.log)
						rf.matchIndex[i] = 0
					}
					rf.mu.Unlock()
					go rf.leaderHeartBeat()
					go rf.allocateAppendCheckers()

				}
			}
			voteMutex.Unlock()
		}(server)
	}

}

func (rf *Raft) ticker() {
	r := rand.New(rand.NewSource(int64(rf.me)))
	//检查是否leader
	for {

		if rf.killed() {
			break
		}
		// Your code here (3A)
		// Check if a leader election should be started.
		timeout := int(r.Float64()*(TIMEOUTHIGH-TIMEOUTLOW) + TIMEOUTLOW)
		rf.mu.Lock()

		if time.Since(rf.timestamp) > time.Duration(timeout)*time.Millisecond && rf.state != LEADER {
			// 选举超时，开始一轮新的选举
			go rf.startElection()
		}
		rf.mu.Unlock()
		// pause for a random amount of time between 50 and 350
		// milliseconds.
		ms := 50 + (rand.Int63() % 300)
		time.Sleep(time.Duration(ms) * time.Millisecond)
	}
}

func (rf *Raft) applyCommited() {
	for {
		rf.mu.Lock()
		for rf.lastApplied >= rf.commitIndex {
			rf.cond.Wait()
		}
		rf.lastApplied++
		DPrintf("[term %d]:Raft [%d] [state %d] apply log entry %d to the service", rf.currentTerm, rf.me, rf.state, rf.lastApplied)
		// DPrintLog(rf)
		cmtidx := rf.lastApplied
		command := rf.log[cmtidx].Command
		rf.mu.Unlock()

		msg := ApplyMsg{
			CommandValid: true,
			Command:      command,
			CommandIndex: cmtidx,
		}

		rf.applyCh <- msg
		DPrintf("[term %d]:Raft [%d] [state %d] apply log entry %d to the service successfully", rf.currentTerm, rf.me, rf.state, rf.lastApplied)
	}
}

// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.

// 服务或测试人员希望创建一个 Raft 服务器。
// 所有 Raft 服务器（包括本服务器）的端口都在 peers[] 中。所有服务器的 peers[] 数组
// 具有相同的顺序。
// persister 是该服务器保存其持久化状态的地方。
// 保存其持久状态的地方，最初也保存最近保存的状态（如果有的话）。
// applyCh 是一个通道，在此通道上
// 测试人员或服务希望 Raft 发送 ApplyMsg 消息的通道。
// Make() 必须快速返回，因此它应该启动 goroutines
// 任何长期运行的工作。

func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (3A, 3B, 3C).

	rf.currentTerm = 0
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.nextIndex = make([]int, len(peers))
	rf.matchIndex = make([]int, len(peers))
	rf.log = make([]LogEntry, 0)
	rf.log = append(rf.log, LogEntry{Term: 0, Command: nil})
	rf.timestamp = time.Now()
	rf.state = FOLLOWER
	rf.votedFor = -1
	rf.cond = sync.NewCond(&rf.mu)
	rf.applyCh = applyCh
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	go rf.applyCommited()
	return rf
}
