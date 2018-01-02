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

import "sync"
import "labrpc"
import "math/rand"
import "time"

// import "bytes"
// import "encoding/gob"

const (
	LEADER    = 0
	CANDIDATE = 1
	FOLLOWER  = 2
)

const (
	HEARTBEAT_CYCLE   = time.Microsecond * 200 // 200ms, 5 heartbeat per second
	ELECTION_MIN_TIME = 400
	ELECTION_MAX_TIME = 600
)

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make().
//
type ApplyMsg struct {
	Index       int
	Command     interface{}
	UseSnapshot bool   // ignore for lab2; only used in lab3
	Snapshot    []byte // ignore for lab2; only used in lab3
}

type LogEntry struct {
	Term    int
	Command interface{}
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	//Persistent state on all servers
	// leaderID    int

	currentTerm int
	votedFor    int
	logs        []LogEntry // need to change in lab2B

	//volatile state on all server
	commitIndex  int
	lastApplied  int
	status       int // 0 : leader; 1 : candidate ; 2 : follower
	grantedVotes int

	//volatile state on leaders
	nextIndex  []int
	matchIndex []int

	timer *time.Timer
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
	term = rf.currentTerm
	isleader = (rf.status == LEADER)
	return term, isleader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := gob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := gob.NewDecoder(r)
	// d.Decode(&rf.xxx)
	// d.Decode(&rf.yyy)
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
}

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term        int //Exchange the term during communicate
	CandidateId int
	// Determine if the candidate's log is up-to-date
	LastLogIndex int
	LastLogTerm  int
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int
	VoteGranted bool
}

type AppendEntiresArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
}

type AppendEntiresReply struct {
	Term    int
	Success bool
}

//
// Code for heartbeat RPC handler.
//
func (rf *Raft) AppendEntries(args AppendEntiresArgs, reply *AppendEntiresReply) {
	// 1. Reply false if term < currentTerm (§5.1)
	// 2. Reply false if log doesn’t contain an entry at prevLogIndex
	// whose term matches prevLogTerm (§5.3)
	// 3. If an existing entry conflicts with a new one (same index
	// but different terms), delete the existing entry and all that
	// follow it (§5.3)
	// 4. Append any new entries not already in the log
	// 5. If leaderCommit > commitIndex, set commitIndex =
	// min(leaderCommit, index of last new entry)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if args.Term < rf.currentTerm {
		reply.Success = false
		reply.Term = rf.currentTerm
		return
	} else {
		//否则收到heartBeart，重置为follower状态,重设计时器状态
		rf.status = FOLLOWER
		rf.votedFor = -1
		rf.currentTerm = args.Term
		reply.Term = rf.currentTerm

		// }else {  //Term相同
		// 	if args.PrevLogIndex >= 0 && (len(rf.logs)-1 <  args.PrevLogTerm || rf.logs[args.PrevLogIndex] != args.PrevLogTerm) {
		// 		reply.Success = false
		// 		reply.Term = rf.currentTerm
		// 		return
		// 	}else{
		// 		rf.logs = rf.logs[:args.PrevLogIndex+1]
		// 		rf.logs = append(rf.logs, args.Entries...)
		// 		if len(rf.logs)-1 >= args.LeaderCommit {
		// 			rf.commitIndex = args.LeaderCommit
		// 			go rf.commitLogs()
		// 		}

		// 		reply.Success = true
		// 	}

		// }

		//否则收到heartBeart，重置为follower状态,重设计时器

		rf.resetTimer()
	}
}

func (rf *Raft) handleAppendEntries(server int, reply AppendEntiresReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.status != LEADER {
		return
	}
	//如果返回term高，需要改变状态
	if reply.Term > rf.currentTerm {
		rf.currentTerm = reply.Term
		rf.status = FOLLOWER
		rf.votedFor = -1
		rf.resetTimer()
		return
	}
	//返回True意味着添加成功
	//if reply.Success {
	//
	//	//返回false意味着添加失败：
	//} else {
	//	rf.nextIndex[server] =
	//		rf.sendHeartbeatToAll()
	//
	//}

	// • If last log index ≥ nextIndex for a follower: send AppendEntries RPC with log entries starting at nextIndex
	// • If successful: update nextIndex and matchIndex for
	// follower (§5.3)
	// • If AppendEntries fails because of log inconsistency:
	// decrement nextIndex and retry (§5.3)
	// • If there exists an N such that N > commitIndex, a majority
	// of matchIndex[i] ≥ N, and log[N].term == currentTerm: set commitIndex = N (§5.3, §5.4).
	//如果返回term和当前term一致，且返回状态为True
	// if reply.Success{

	// }else{

	// }

}

func (rf *Raft) sendAppendEntries(server int, args AppendEntiresArgs, reply *AppendEntiresReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

func (rf *Raft) sendHeartbeatToAll() {
	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}
		var args AppendEntiresArgs

		args.Term = rf.currentTerm
		args.LeaderId = rf.me
		args.PrevLogIndex = rf.nextIndex[rf.me] - 1
		if args.PrevLogIndex >= 0 {
			args.PrevLogTerm = rf.logs[args.PrevLogIndex].Term
		}
		if rf.nextIndex[i] < len(rf.logs) {
			args.Entries = rf.logs[rf.nextIndex[i]:]
		}
		args.LeaderCommit = rf.commitIndex
		//难道这个goroutine不能直接访问参数吗
		go func(server int, args AppendEntiresArgs) {
			var reply AppendEntiresReply
			ok := rf.sendAppendEntries(server, args, &reply)
			if ok {
				rf.handleAppendEntries(server, reply)
			}
		}(i, args)
	}
}

//
// example RequestVote RPC handler.
//

func (rf *Raft) RequestVote(args RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	// 1. Reply false if term < currentTerm (§5.1)
	// 2. If votedFor is null or candidateId, and candidate’s log is at
	// least as up-to-date as receiver’s log, grant vote (§5.2, §5.4)
	rf.mu.Lock()
	defer rf.mu.Unlock()

	may_grant_vote := true

	// current server's log must newer than the candidate
	if len(rf.logs) > 0 {
		if rf.logs[len(rf.logs)-1].Term > args.LastLogTerm ||
			(rf.logs[len(rf.logs)-1].Term == args.LastLogTerm &&
				len(rf.logs)-1 > args.LastLogIndex) {
			may_grant_vote = false
		}
	}

	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return
	}

	if args.Term == rf.currentTerm {
		if rf.votedFor == -1 && may_grant_vote {
			rf.votedFor = args.CandidateId
			//rf.persist()
		}
		reply.Term = rf.currentTerm
		reply.VoteGranted = (rf.votedFor == args.CandidateId)
		//if reply.VoteGranted {
		//	rf.logger.Printf("Vote for server[%v] at Term:%v\n", rf.me, args.CandidateId, rf.currentTerm)
		//}
		return
	}

	if args.Term > rf.currentTerm {
		rf.status = FOLLOWER
		rf.votedFor = -1
		rf.currentTerm = args.Term
		if may_grant_vote {
			rf.votedFor = args.CandidateId
			//rf.persist()
		}
		rf.resetTimer()
		reply.Term = args.Term
		reply.VoteGranted = (rf.votedFor == args.CandidateId)
		//if reply.VoteGranted {
		//	rf.logger.Printf("Vote for server[%v] at Term:%v\n", rf.me, args.CandidateId, rf.currentTerm)
		//}
		return
	}

}

func (rf *Raft) handleVoteResult(reply RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	//其实并不会发生吗？
	if reply.Term < rf.currentTerm {
		return
	}

	if reply.Term > rf.currentTerm {
		rf.currentTerm = reply.Term
		rf.status = FOLLOWER
		rf.votedFor = -1
		rf.grantedVotes = 0
		rf.resetTimer()
		return
	}

	if rf.status == CANDIDATE && reply.VoteGranted {
		rf.grantedVotes += 1
		if rf.grantedVotes >= majority(len(rf.peers)) {
			rf.status = LEADER
			//rf.logger.Printf("Leader at Term:%v log_len:%v\n", rf.me, rf.currentTerm, len(rf.logs))
			for i := 0; i < len(rf.peers); i++ {
				if i == rf.me {
					continue
				}
				rf.nextIndex[i] = len(rf.logs)
				rf.matchIndex[i] = -1
			}
			rf.resetTimer()
		}
		return
	}
}

//
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
//
func (rf *Raft) sendRequestVote(server int, args RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).

	return index, term, isLeader
}

//
// the tester calls Kill() when a Raft instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (rf *Raft) Kill() {
	// Your code here, if desired.
}

func (rf *Raft) resetTimer() {
	if rf.status != LEADER {
		timeout := time.Duration(ELECTION_MIN_TIME+rand.Int63n(ELECTION_MAX_TIME-ELECTION_MIN_TIME)) * time.Microsecond
		rf.timer.Reset(timeout)
	} else {
		rf.timer.Reset(HEARTBEAT_CYCLE)
	}
}

func (rf *Raft) beginElection() {
	// On conversion to candidate, start election:
	// • Increment currentTerm
	// • Vote for self
	// • Reset election timer
	// • Send RequestVote RPCs to all other servers
	// • If votes received from majority of servers: become leader
	// • If AppendEntries RPC received from new leader: convert to
	// follower
	// • If election timeout elapses: start new election
	rf.status = CANDIDATE
	rf.currentTerm = rf.currentTerm + 1
	rf.votedFor = rf.me
	rf.grantedVotes = 1

	args := RequestVoteArgs{
		Term:        rf.currentTerm,
		CandidateId: rf.me,
	}

	for server := 0; server < len(rf.peers); server++ {
		if server == rf.me {
			continue
		}

		go func(server int, args RequestVoteArgs) {
			var reply RequestVoteReply
			ok := rf.sendRequestVote(server, args, &reply)
			if ok {
				rf.handleVoteResult(reply)
			}
		}(server, args)

	}
	rf.resetTimer()
}

func majority(n int) int {
	return n/2 + 1
}

func (rf *Raft) initTimer() {
	//在主线程为Leader、follower、candidata 初始化timer
	if rf.timer == nil {
		rf.timer = time.NewTimer(HEARTBEAT_CYCLE)
		if rf.status != LEADER {
			timeout := time.Duration(ELECTION_MIN_TIME+rand.Int63n(ELECTION_MAX_TIME-ELECTION_MIN_TIME)) * time.Microsecond
			rf.timer.Reset(timeout)
		}
	}
	//启动一个Goroutine，为Leader、follower、candidata分配不同的执行任务
	go func() {
		for {
			//等待timer到期
			<-rf.timer.C
			if rf.status == LEADER {
				rf.sendHeartbeatToAll()
			} else {
				rf.beginElection()
			}
		}
	}()
}

//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//

func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).

	rf.currentTerm = 0
	rf.votedFor = -1
	rf.logs = make([]LogEntry, 0)

	rf.commitIndex = -1
	rf.lastApplied = -1

	rf.nextIndex = make([]int, len(peers))
	rf.matchIndex = make([]int, len(peers))
	rf.grantedVotes = 0
	rf.status = FOLLOWER

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// 使用一个goroutine初始化周期性任务函数，如启动选举、发送heartBeat；使主函数尽快返回
	rf.initTimer()

	return rf
}
