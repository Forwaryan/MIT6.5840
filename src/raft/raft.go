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

	"bytes"
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	//	"6.5840/labgob"
	"6.5840/labgob"
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

const LogOption = false

func (rf *Raft) rflog(format string, args ...interface{}) {
	// file, err := os.OpenFile("log", os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	// if err != nil {
	// 	fmt.Println("Error creating file:", err)
	// 	return
	// }

	// defer file.Close()

	if LogOption {
		format = fmt.Sprintf("[%d] ", rf.me) + format
		fmt.Printf(format, args...)
		fmt.Println("")
		// loger.Printf(format, args...)
	}
}

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
	//持久化变量
	currentTerm int
	voteFor     int
	log         []LogEntry

	state RuleState

	commitIndex int
	lastApplied int
	nextIndex   []int
	matchIndex  []int

	electionStartTime time.Time
	applyChan         chan ApplyMsg
	commitCond        *sync.Cond
}

type RuleState int

const (
	Follower RuleState = iota
	Candidate
	Leader
	Dead
)

// 涉及到快照 所以真实下标和在日志中的下标是不同的
// 日志的第一项充当记录，保存LastIncludedIndex 和 LastIncludedTerm
type LogEntry struct {
	Command interface{}
	Term    int
	Index   int
}

func (s RuleState) String() string {
	switch s {
	case Follower:
		return "Follower"
	case Candidate:
		return "Candidate"
	case Leader:
		return "Leader"
	case Dead:
		return "Dead"
	default:
		panic("unreachable")
	}
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	rf.mu.Lock()
	defer rf.mu.Unlock()
	// Your code here (3A).
	return rf.currentTerm, rf.state == Leader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).

// encode
func (rf *Raft) encodeState() []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.voteFor)
	e.Encode(rf.log)
	raftstate := w.Bytes()
	return raftstate
}

func (rf *Raft) persist() {
	// Your code here (3C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// raftstate := w.Bytes()
	rf.persister.Save(rf.encodeState(), rf.persister.ReadSnapshot())
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (3C).
	// Example:
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var currentTerm int
	var vorteFor int
	var log []LogEntry
	if d.Decode(&currentTerm) != nil || d.Decode(&vorteFor) != nil || d.Decode(&log) != nil {
		//任何一个解码操作失败 直接return
		return
	} else {
		rf.mu.Lock()
		rf.currentTerm = currentTerm
		rf.voteFor = vorteFor
		rf.log = log
		rf.mu.Unlock()
	}
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
	rf.mu.Lock()
	defer rf.mu.Unlock()

	rf.rflog("snapshot index %d", index)
	lastIndex := rf.getFirstIndex()
	if lastIndex >= index {
		//已经做过快照了
		return
	}

	var tmp []LogEntry
	//将没创建过快照的日志乡村放到tmp中，然后赋值给rf.log(覆盖)
	rf.log = append(tmp, rf.log[index-lastIndex:]...)
	//第0个日志存快照信息
	//lastIncludedIndex 和 lastIncludeTerm 就是下标为 index 的日志的信息
	//因此裁剪时保留它充当快照信息
	rf.log[0].Command = nil
	rf.persister.Save(rf.encodeState(), snapshot)
}

type InstallSnapshotArgs struct {
	Term              int
	LeaderId          int
	LastIncludedIndex int //快照中最后一个条目的索引
	LastIncludedTerm  int //快照中最后一个条目的任期
	Snapshot          []byte
}

type InstallSnapshotReply struct {
	Term int
}

func (rf *Raft) sendInstallSnapshot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	ok := rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
	return ok
}

func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	if rf.state == Dead {
		rf.mu.Unlock()
		return
	}

	rf.rflog("receives InstallSnapshot [%v]", args)
	if args.Term > rf.currentTerm {
		rf.rflog("term is out of data in InstallSnapshot")
		rf.becomeFollower(args.Term)
		rf.electionStartTime = time.Now()
	}

	reply.Term = rf.currentTerm
	if args.Term == rf.currentTerm {
		if rf.state != Follower {
			rf.state = Follower
		}
		rf.electionStartTime = time.Now()

		//因为延迟得到过期的快照
		if rf.commitIndex >= args.LastIncludedIndex {
			rf.rflog("receive out of data snapshot, commitIndex: [%d], args.LsdtIncludedIndex: [%d]", rf.commitIndex, args.LastIncludedIndex)
			rf.mu.Unlock()
			return
		}

		//裁剪日志，将已经保存到快照的日志删除
		if rf.getLastIndex() <= args.LastIncludedIndex {
			//将以保存到快照中的日志删除
			rf.log = make([]LogEntry, 1)
		} else {
			var tmp []LogEntry
			rf.log = append(tmp, rf.log[args.LastIncludedIndex-rf.getFirstIndex():]...)
		}

		rf.log[0].Term = args.LastIncludedTerm
		rf.log[0].Index = args.LastIncludedIndex
		rf.log[0].Command = nil
		//rf的log修改后，将persist变量持久化存储，并保存快照
		//server将leader中的快照保存到自己的存储中
		rf.persister.Save(rf.encodeState(), args.Snapshot)

		rf.rflog("persist on InstallSnapshot over!, term is %d, log is %d", rf.currentTerm, rf.log)
		rf.lastApplied = args.LastIncludedIndex
		rf.commitIndex = args.LastIncludedIndex
		rf.mu.Unlock()
		rf.applyChan <- ApplyMsg{
			SnapshotValid: true,
			Snapshot:      args.Snapshot,
			SnapshotTerm:  args.LastIncludedTerm,
			SnapshotIndex: args.LastIncludedIndex,
		}
		rf.rflog("InstallSnapshot over!")
		return
	}

	rf.mu.Unlock()
}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (3A, 3B).
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (3A).
	Term        int
	VoteGranted bool
}

// 每个都会新启动新的选举定时器
func (rf *Raft) becomeFollower(term int) {
	rf.state = Follower
	rf.currentTerm = term
	rf.voteFor = -1
	rf.persist()
	rf.rflog("becomes follower at term [%d]", term)
	go rf.ticker(Follower)
}

func (rf *Raft) becomeLeader() {
	rf.state = Leader
	rf.rflog("becomes leader at term [%d]", rf.currentTerm)
	// 待匹配节点设置为最后一个日志的下一个 如果不匹配再向前跳  直到匹配
	nextIndex := rf.getNextIndex()
	for i := range rf.peers {
		rf.nextIndex[i] = nextIndex
		rf.matchIndex[i] = 0
	}
	go rf.ticker(Leader)
}

// 返回最后一个日志的下标
func (rf *Raft) getLastIndex() int {
	return rf.log[len(rf.log)-1].Index
}

// 返回最后一个日志的任期
func (rf *Raft) getLastTerm() int {
	return rf.log[len(rf.log)-1].Term
}

// 返回第一个日志的下标
func (rf *Raft) getFirstIndex() int {
	return rf.log[0].Index
}

// 返回第一个日志的任期
func (rf *Raft) getFirstTerm() int {
	return rf.log[0].Term
}

// 返回下标为 index 处的日志的任期
func (rf *Raft) getTerm(index int) int {
	return rf.log[index-rf.getFirstIndex()].Term
}

// 返回最后一个日志的下一个下标
func (rf *Raft) getNextIndex() int {
	return rf.getLastIndex() + 1
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (3A, 3B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.state == Dead {
		return
	}

	rf.rflog("is requested vote, args [%+v]; currentTerm : %d, voteFor: %d, log: [%v]",
		args, rf.currentTerm, rf.voteFor, rf.log)

	if args.Term > rf.currentTerm {
		rf.rflog("term is out of data in RequestVote")
		//领导者任期大于server的任期,server只能成为follower
		if rf.state != Follower {
			rf.electionStartTime = time.Now()
		}
		rf.becomeFollower(args.Term)
	}

	reply.VoteGranted = false
	lastLogIndex := rf.getLastIndex()
	lastLogTerm := rf.getLastTerm()
	need_persist := false

	if rf.currentTerm == args.Term {
		if rf.voteFor == -1 || rf.voteFor == args.CandidateId {
			// 至少要保证候选者的日志比server新
			if args.LastLogTerm > lastLogTerm || (args.LastLogTerm == lastLogTerm && args.LastLogIndex >= lastLogIndex) {
				reply.VoteGranted = true
				rf.electionStartTime = time.Now()

				if rf.voteFor == -1 {
					rf.voteFor = args.CandidateId
					need_persist = true
				}
			}

		}

	}

	if need_persist {
		rf.persist()
	}
	reply.Term = rf.currentTerm
	rf.rflog("qqq : reply in RequestVote [%+v] to [%d]", reply, args.CandidateId)

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

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term    int
	Success bool
	//避免Leader每次日志不匹配只往前移动一次;若日志很长的话在一段时间内无法到达冲突位置
	ConflictIndex int
	ConflictTerm  int
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

// 第一个返回值是该命令在提交时将出现的索引。
// 第二个返回值是当前的任期。
// 第三个返回值是 true，如果该服务器认为自己是 the leader.

// 添加命令，非Leader时会直接返回false
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	// index := -1
	// term := -1
	// isLeader := true

	// Your code here (3B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.state != Leader {
		return -1, rf.currentTerm, false
	}

	rf.rflog("receives commond %v", command)

	rf.log = append(rf.log, LogEntry{
		Term:    rf.currentTerm,
		Command: command,
		Index:   rf.getNextIndex(),
	})
	rf.persist()
	// rf.rflog("start aaaaaa : %d", rf.getLastIndex())
	return rf.getLastIndex(), rf.currentTerm, true
	// return index, term, isLeader
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
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.state = Dead
	rf.rflog("state dead")

}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

// 心跳定时器 100ms发一次
func (rf *Raft) heartBeatsTimer() {
	rf.mu.Lock()
	nowTerm := rf.currentTerm
	rf.mu.Unlock()
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	for !rf.killed() {
		<-ticker.C
		rf.mu.Lock()
		if rf.state != Leader || rf.currentTerm != nowTerm {
			rf.mu.Unlock()
			return
		}
		rf.runHeartBeats()
		rf.mu.Unlock()
	}
}

func (rf *Raft) ticker(state RuleState) {
	if !rf.killed() {

		// Your code here (3A)
		// Check if a leader election should be started.

		// pause for a random amount of time between 50 and 350
		// milliseconds.
		switch state {
		case Follower:
			rf.runElectionTimer()
		case Candidate:
			rf.runElectionTimer()

		case Leader:
			rf.heartBeatsTimer()
		}

		// ms := 50 + (rand.Int63() % 300)
		// time.Sleep(time.Duration(ms) * time.Millisecond)
	}
}

// 超时选举 [150, 300]
// 利用定时器不断检查，变为Leader后直接结束
// 注意，我们一定要保持任期是一致的，若落后了表明当前协程是上个任期运行的定时器，直接结束
// 一直满足条件的话，就等待超时后变为候选者进行选举
func (rf *Raft) runElectionTimer() {
	ms := 250 + (rand.Int63() % 150)
	timeout := time.Duration(ms) * time.Millisecond
	rf.mu.Lock()
	nowTerm := rf.currentTerm
	rf.mu.Unlock()
	// rf.rflog("election timer start, timeout (%v), now term = (%v)", timeout, nowTerm)

	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()

	for !rf.killed() {
		<-ticker.C
		rf.mu.Lock()
		// rf.rflog("after %v, timeout is %v, currentTerm [%d], realTerm [%d], state [%s]",time.Since(rf.electionStartTime), timeout, nowTerm, rf.currentTerm, rf.state.String())

		if rf.state != Candidate && rf.state != Follower {
			// rf.rflog("in runElectionTimer, state change to %s, currentTerm [%d], realTerm [%d]", rf.state.String(), nowTerm, rf.currentTerm)

			rf.mu.Unlock()
			return
		}

		if nowTerm != rf.currentTerm {
			// rf.rflog("in runElectionTimer, term change from %d to %d, currentTerm [%d], realTerm [%d]", nowTerm, rf.currentTerm, nowTerm, rf.currentTerm)

			rf.mu.Unlock()
			return
		}

		if duration := time.Since(rf.electionStartTime); duration >= timeout {
			// rf.rflog("timeed out !! timer after %v, currentTerm [%d], realTerm [%d]", time.Since(rf.electionStartTime), nowTerm, rf.currentTerm)
			rf.startElection()
			rf.mu.Unlock()
			continue
		}
		rf.mu.Unlock()
	}

}

func (rf *Raft) startElection() {

	rf.currentTerm += 1
	rf.state = Candidate
	rf.voteFor = rf.me
	rf.electionStartTime = time.Now()
	rf.persist()
	// rf.rflog("becomes Candidate, start election! now term is %d", rf.currentTerm)

	receivedVotes := 1

	args := RequestVoteArgs{
		Term:         rf.currentTerm,
		CandidateId:  rf.me,
		LastLogIndex: rf.getLastIndex(),
		LastLogTerm:  rf.getLastTerm(),
	}

	for server := range rf.peers {
		if server == args.CandidateId {
			continue
		}

		go func(server int) {
			var reply RequestVoteReply
			if success := rf.sendRequestVote(server, &args, &reply); success {
				// rf.rflog("receive requestVote reply [%+v]", reply)
				rf.mu.Lock()
				defer rf.mu.Unlock()

				// 当前节点状态仍然是candidate
				// 当前的任期仍然等于之前的任期
				if rf.state == Candidate && rf.currentTerm == args.Term {
					if reply.VoteGranted {
						receivedVotes += 1
						if receivedVotes*2 >= len(rf.peers)+1 {
							rf.rflog("id : [%d] wins the selection, becomes leader!", rf.me)
							rf.becomeLeader()
							rf.runHeartBeats()
						}
					} else if reply.Term > rf.currentTerm {
						rf.rflog("receive bigger term in reply, maybe out of data")
						rf.becomeFollower(reply.Term)
						rf.electionStartTime = time.Now()
					}
				}

			}
		}(server)
	}

	// 参考代码：因为锁的抢占问题可能 ticker() 中获取状态时已经变成 leader 了，进而存在两个心跳计时器，因此指定开启哪个定时器
	go rf.ticker(Follower)
}

// Lab3B 完善 AppendEntriesArgs 发送信息
// 收到请求后会再检查一次该日志是否过半，能否commit
func (rf *Raft) runHeartBeats() {
	if rf.state != Leader {
		rf.rflog("not leader, return")
		return
	}

	currentTerm := rf.currentTerm
	rf.rflog("leader - - - - ticker!!!--------run runHeartBeats()")

	for server := range rf.peers {
		if server == rf.me {
			continue
		}

		go func(server int) {
			for !rf.killed() {
				rf.mu.Lock()
				//prev上一个匹配的日志
				if rf.state != Leader {
					rf.mu.Unlock()
					return
				}

				firstIndex := rf.getFirstIndex()
				//想要发送的日志已经被删除了，需要快照来恢复
				if rf.nextIndex[server] <= firstIndex {
					rf.rflog("send snapshot to %d, nextIndex is [%d] but lastIncludedIndex is [%d]", server, rf.nextIndex[server], firstIndex)
					args := InstallSnapshotArgs{
						Term:              currentTerm,
						LeaderId:          rf.me,
						LastIncludedTerm:  rf.getFirstTerm(),
						LastIncludedIndex: firstIndex,
						Snapshot:          rf.persister.ReadSnapshot(),
					}
					rf.mu.Unlock()
					var reply InstallSnapshotReply
					rf.rflog("sending InstallSnapshotArgs to [%v], args = [%+v]", server, args)
					if rf.sendInstallSnapshot(server, &args, &reply) {
						rf.rflog("receive InstallSnapshot reply [%+v]", reply)
						rf.mu.Lock()
						//更新matchIndex和nextIndex
						rf.handleInstallSnapshotRPCResponse(server, &args, &reply)
						rf.mu.Unlock()
					}
					return
				} else {
					prevLogIndex := rf.nextIndex[server] - 1
					nowLogIndex := rf.nextIndex[server]
					// rf.rflog("rqrqrqrt %d", prevLogIndex)
					entries := make([]LogEntry, rf.getNextIndex()-nowLogIndex)
					// rf.rflog("??logentry :xxxx %d, :  %d", rf.getNextIndex(), nowLogIndex)
					copy(entries, rf.log[nowLogIndex-rf.getFirstIndex():])
					args := AppendEntriesArgs{
						Term:         currentTerm,
						LeaderId:     rf.me,
						PrevLogIndex: prevLogIndex,
						PrevLogTerm:  rf.getTerm(prevLogIndex),
						Entries:      entries,
						LeaderCommit: rf.commitIndex,
					}

					rf.mu.Unlock()
					var reply AppendEntriesReply
					rf.rflog("sending AppendEntries to [%v], args = [%+v]", server, args)
					if rf.sendHeartBeats(server, &args, &reply) {
						rf.rflog("Leader - - - - receive AppendEntries reply [%+v]", reply)
						rf.mu.Lock()

						if rf.handleAppendEntriesRPCResponse(server, &args, &reply) {
							rf.mu.Unlock()
							continue
						}
						rf.mu.Unlock()
					}
					return
				}

			}
		}(server)
	}
}

func (rf *Raft) handleInstallSnapshotRPCResponse(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	if rf.state == Leader && rf.currentTerm == args.Term {
		if reply.Term == rf.currentTerm {
			rf.rflog("receives reply from [%v], nextIndex changes from [%v] to [%v]",
				server, rf.nextIndex[server], args.LastIncludedIndex+1)
			rf.matchIndex[server] = max(rf.matchIndex[server], args.LastIncludedIndex)
			rf.nextIndex[server] = rf.matchIndex[server] + 1
		} else if reply.Term > rf.currentTerm {
			rf.rflog("receive bigger term in reply, transforms to follower")
			rf.becomeFollower(reply.Term)
			rf.electionStartTime = time.Now()
		}
	}
}

func (rf *Raft) handleAppendEntriesRPCResponse(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	if rf.state == Leader && rf.currentTerm == args.Term {
		if reply.Term == rf.currentTerm {
			if reply.Success {
				rf.matchIndex[server] = max(rf.matchIndex[server], args.PrevLogIndex+len(args.Entries))
				rf.nextIndex[server] = rf.matchIndex[server] + 1
				rf.rflog("receives reply from [%v], nextIndex := [%v], matchIndex := [%v]",
					server, rf.nextIndex[server], rf.matchIndex[server])
				// 统计投票结果, 更新 commitIndex\
				// rf.rflog("??????????????? %d", rf.commitIndex)
				savedCommitIndex := rf.commitIndex
				// rf.rflog("now ::aaaaaaaaaaaaaaaaa %d, idxxxxxxx : %d", rf.commitIndex+1, rf.getNextIndex())
				for i := rf.commitIndex + 1; i < rf.getNextIndex(); i++ {
					if rf.getTerm(i) == rf.currentTerm {
						count := 1
						for j := range rf.peers {
							if j != rf.me && rf.matchIndex[j] >= i {
								count++
							}
						}
						// rf.rflog("now ppppp ::tttttttttttt %d, idxxxxxxx : %d", count, i)
						if count*2 >= len(rf.peers)+1 {
							// rf.rflog("ttttttttttt !!!!!!!!!!!!!! %d,  now  idx", cout, i)
							rf.commitIndex = i
						} else {
							break
						}
					}
				}
				if rf.commitIndex != savedCommitIndex {
					rf.rflog("updates commitIndex from %v to %v", savedCommitIndex, rf.commitIndex)
					rf.commitCond.Signal()
				}

			} else {
				rf.rflog("receives reply from[%v] failed", server)
				if reply.ConflictTerm > 0 {
					//对应PrevLogIndex < rf.getNextIndex - 跳到不冲突Term
					lastIndex := -1
					firstIndex := rf.getFirstIndex()
					for i := args.PrevLogIndex - 1; i >= firstIndex; i-- {
						if rf.getTerm(i) == reply.ConflictTerm {
							lastIndex = i
							break
						} else if rf.getTerm(i) < reply.ConflictTerm {
							break
						}
					}
					//lastIndex是跳到与leader Prev 相等的Term的第一个
					//reply.ConflictIndex是跳到和这个server term相等的最后一个
					if lastIndex > 0 {
						rf.nextIndex[server] = lastIndex + 1
					} else {
						rf.nextIndex[server] = max(reply.ConflictIndex, rf.matchIndex[server]+1)
					}

				} else {
					rf.nextIndex[server] = max(reply.ConflictIndex, rf.matchIndex[server]+1)
				}
				//这里为什么要返回true?
				//返回true的情况下为了让他重新发日志
				rf.rflog("receives reply from [%v] failed, nextIndex changes to [%d]", server, rf.nextIndex[server])
				return true
			}
		} else if reply.Term > rf.currentTerm {
			rf.rflog("receive bigger term in reply, transforms to follower")
			rf.becomeFollower(reply.Term)
			rf.electionStartTime = time.Now()
		}

	}

	return false
}

func (rf *Raft) sendHeartBeats(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.state == Dead {
		return
	}
	rf.rflog("receives AppendEntries [Term:%d LeaderId:%d PrevLogIndex:%d PrevLogTerm:%d Entries:%d LeaderCommit:%d]",
		args.Term, args.LeaderId, args.PrevLogIndex, args.PrevLogTerm, len(args.Entries), args.LeaderCommit)

	need_persist := false

	if args.Term > rf.currentTerm {
		rf.rflog("now have leader %d , transforms to follower %d", args.LeaderId, rf.me)
		rf.becomeFollower(args.Term)
		rf.electionStartTime = time.Now()
	}

	reply.Success = false

	if args.Term == rf.currentTerm {
		rf.state = Follower
		rf.electionStartTime = time.Now()

		//远程调用的日志信息已经在当前raft实例中过时，因为在当前raft中不存在该索引的日志
		if args.PrevLogIndex < rf.getFirstIndex() {
			rf.rflog("receives out of data AppendEntries RPC, args.PrevLogIndex [%d], LastIncludedIndex [%d]", args.PrevLogIndex, rf.getFirstIndex())
			reply.Success = false
			reply.Term = 0
			return
		}

		// rf.rflog("tqtqtqtq %d", args.PrevLogIndex)

		if args.PrevLogIndex < rf.getNextIndex() && args.PrevLogTerm == rf.getTerm(args.PrevLogIndex) {
			reply.Success = true
			insertIndex := args.PrevLogIndex + 1
			argsLogIndex := 0
			//找到server和leader不匹配的第一个日志的索引
			//跳过去的++的索引都是二者匹配的
			for {
				if insertIndex >= rf.getNextIndex() || argsLogIndex >= len(args.Entries) ||
					rf.getTerm(insertIndex) != args.Entries[argsLogIndex].Term {
					break
				}

				insertIndex++
				argsLogIndex++
			}

			// rf.rflog("?????????:  append logs %d,  :,   %d", argsLogIndex, len(args.Entries))
			// rf.rflog("?????????: oooooo  %d,  :,   %d", insertIndex, argsLogIndex)

			// 从第一个不匹配的索引开始，将args的entries采用覆盖/追加的方式加入到server的日志中
			if argsLogIndex < len(args.Entries) {
				rf.log = append(rf.log[:insertIndex-rf.getFirstIndex()], args.Entries[argsLogIndex:]...)
				need_persist = true
				rf.rflog("append logs [%v] in AppendEntries", args.Entries[argsLogIndex:])
			}

			// 检查是否需要提交命令
			// rf.rflog("xxxxxxxxxxxxxxxxxx :%d  --- %d", args.LeaderCommit, rf.commitIndex)
			if args.LeaderCommit > rf.commitIndex {
				rf.commitIndex = min(rf.getNextIndex()-1, args.LeaderCommit)
				rf.rflog("updates commitIndex into %v", rf.commitIndex)
				rf.commitCond.Signal()
			}
		} else {
			if args.PrevLogIndex >= rf.getNextIndex() {
				reply.ConflictIndex = rf.getNextIndex()
				reply.ConflictTerm = -1
			} else {
				//PrevLogIndex < rf.getNextIndex
				reply.ConflictTerm = rf.getTerm(args.PrevLogIndex)
				var index int
				//当前leader term != server term

				for index = args.PrevLogIndex - 1; index >= rf.getFirstIndex(); index-- {
					if rf.getTerm(index) != reply.ConflictTerm {
						break
					}
				}
				reply.ConflictIndex = index + 1
			}
		}
	}

	if need_persist {
		rf.persist()
	}

	reply.Term = rf.currentTerm
	rf.rflog("reply AppendEntries [%+v] to %d", reply, args.LeaderId)
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
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (3A, 3B, 3C).
	rf.currentTerm = 0
	rf.voteFor = -1
	rf.state = Follower
	rf.dead = 0
	rf.electionStartTime = time.Now()

	rf.log = make([]LogEntry, 1)
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	rf.commitIndex = rf.getFirstIndex()
	rf.lastApplied = rf.getFirstIndex()
	rf.nextIndex = make([]int, len(peers))
	rf.matchIndex = make([]int, len(peers))

	for i := range rf.nextIndex {
		rf.nextIndex[i] = rf.getNextIndex()
		rf.matchIndex[i] = 0
	}
	rf.applyChan = applyCh
	rf.commitCond = sync.NewCond(&rf.mu)
	// start ticker goroutine to start elections
	go rf.ticker(Follower)

	//检查是否需要提交命令
	go rf.commitCommand()
	return rf
}

// 提交命令给test程序
// rf.lastApplied >= rf.commitIndex 时调用wait
func (rf *Raft) commitCommand() {
	for !rf.killed() {
		rf.mu.Lock()
		for rf.lastApplied >= rf.commitIndex {
			rf.commitCond.Wait()
		}

		logEntries := make([]LogEntry, rf.commitIndex-rf.lastApplied)
		firstIndex := rf.getFirstIndex()
		commitIndex := rf.commitIndex
		copy(logEntries, rf.log[rf.lastApplied+1-firstIndex:rf.commitIndex+1-firstIndex])
		rf.rflog("commits log from %d (%d) to %d (%d)", rf.lastApplied-firstIndex, rf.lastApplied, rf.commitIndex-firstIndex, rf.commitIndex)
		rf.lastApplied = max(rf.lastApplied, commitIndex)

		rf.mu.Unlock()
		rf.rflog("commit log: [%v]", logEntries)

		for _, entry := range logEntries {
			rf.applyChan <- ApplyMsg{
				CommandValid: true,
				Command:      entry.Command,
				CommandIndex: entry.Index,
			}
		}
		rf.rflog("commits log from over !!")
	}
}

func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}
