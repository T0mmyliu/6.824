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
import "fmt"
import "math/rand"
import "time"
import "strconv"

// import "bytes"
// import "labgob"

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in Lab 3 you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh; at that point you can add fields to
// ApplyMsg, but set CommandValid to false for these other uses.
//
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
}

const (
	Follower  = 0
	Candidate = 1
	Leader    = 2
)

type Log struct {
	Commnad interface{}
	Index   int
	Term    int
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

	// persistent state
	curTerm  int
	votedFor int
	log      []Log

	// volatile state
	commitIndex int
	lastApplied int

	//
	identity int

	// state for leader
	nextIndex  []int
	matchIndex []int

	//
	electionTimeout      int
	heartbeatQuitCh      chan bool
	leaderElectionQuitCh chan bool
	applyQuitCh          chan bool
	applyCh              chan ApplyMsg
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	var term int = rf.curTerm
	var isleader bool = rf.identity == Leader
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
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
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

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	PeerIndex int
	CurTerm   int
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	CurTerm   int
	PeerIndex int
	IsAccept  bool
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	fmt.Println("node: " + strconv.Itoa(rf.me) + " receive vote request")

	if rf.curTerm < args.CurTerm {
		if rf.identity == Leader {
			rf.identity = Follower
			rf.heartbeatQuitCh <- true
			go LeaderElectionFunc(rf)
		}
		rf.votedFor = args.PeerIndex
		rf.curTerm = args.CurTerm
		rf.electionTimeout = GenLeaderElectionTimeout()
		reply.CurTerm = rf.curTerm
		reply.IsAccept = true
		reply.PeerIndex = rf.me
		fmt.Println(strconv.Itoa(rf.me) + " agree " + strconv.Itoa(args.PeerIndex) + " term " + strconv.Itoa(args.CurTerm))
	} else {
		fmt.Println(strconv.Itoa(rf.me) + " rej " + strconv.Itoa(args.PeerIndex) + "args term " + strconv.Itoa(args.CurTerm) + " cur term:" + strconv.Itoa(rf.curTerm))
		reply.IsAccept = false
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
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	fmt.Println(strconv.Itoa(rf.me) + " send to " + strconv.Itoa(server))
	finish := make(chan bool)
	go func() {
		rf.peers[server].Call("Raft.RequestVote", args, reply)
		finish <- true
	}()
	go func() {
		time.Sleep(10 * time.Millisecond)
		fmt.Println("send vote timeout." + strconv.Itoa(rf.me) + " send to " + strconv.Itoa(server))
		reply.IsAccept = false
		finish <- true
	}()
	select {
	case <-finish:
		return true
	default:
		time.Sleep(5 * time.Millisecond)
	}
	return true
}

type HeartbeatArgs struct {
	Term         int
	PeerIndex    int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []Log
	LeaderCommit int
}

type HeartbeatReply struct {
	Term    int
	Success bool
}

func (rf *Raft) Heartbeat(args *HeartbeatArgs, reply *HeartbeatReply) {
	if args.Term > rf.curTerm {
		fmt.Println(strconv.Itoa(args.Term) + " " + strconv.Itoa(rf.curTerm) + " " + strconv.Itoa(rf.identity))
		if rf.identity == Leader {
			rf.identity = Follower
			rf.heartbeatQuitCh <- true
		}
		rf.curTerm = args.Term
	}
	//fmt.Println("receive heartbeat from " + strconv.Itoa(args.PeerIndex))
	rf.electionTimeout = GenLeaderElectionTimeout()
}

func (rf *Raft) sendHeartbeat(server int, args *HeartbeatArgs, reply *HeartbeatReply) bool {
	ok := rf.peers[server].Call("Raft.Heartbeat", args, reply)
	return ok
}

func (rf *Raft) AppendEntries(args *HeartbeatArgs, reply *HeartbeatReply) {
	if args.PrevLogIndex != args.Entries[0].Index {
		reply.Success = false
		return
	}
	rf.log = append(rf.log, args.Entries[0])
	reply.Success = true
	reply.Term = rf.curTerm
}

func (rf *Raft) sendAppendEntries(server int, args *HeartbeatArgs, reply *HeartbeatReply) {
	rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return
}

type CommitArgs struct {
	Index int
}

type CommitReply struct {
}

func (rf *Raft) Commit(args *CommitArgs, reply *CommitReply) {
	rf.commitIndex = args.Index
	return
}

func (rf *Raft) sendCommit(server int, commitIndex int) {
	args := &CommitArgs{commitIndex}
	reply := &CommitReply{}
	rf.peers[server].Call("Raft.Commit", args, reply)
	return
}

//
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
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).
	if rf.identity != Leader {
		isLeader = false
	} else {

		term = rf.curTerm
		isLeader = true

		if rf.nextIndex == nil || len(rf.nextIndex) == 0 {
			if rf.log == nil || len(rf.log) == 0 {
				rf.nextIndex = make([]int, len(rf.peers))
				for i, _ := range rf.nextIndex {
					rf.nextIndex[i] = 1
				}
			} else {
				rf.nextIndex = make([]int, len(rf.peers))
				for i, _ := range rf.nextIndex {
					rf.nextIndex[i] = rf.log[len(rf.log)-1].Index + 1
				}
			}
		}
		// TODO:sync log
		log := Log{command, rf.nextIndex[rf.me], rf.curTerm}
		rf.log = append(rf.log, log)
		rf.nextIndex[rf.me] = log.Index + 1

		cnt := 0
		entries := make([]Log, 0)
		entries = append(entries, log)

		args := &HeartbeatArgs{rf.curTerm, rf.me, log.Index, log.Term, entries, rf.commitIndex}
		for i := 0; i < len(rf.peers); i++ {
			if i == rf.me {
				continue
			}
			reply := &HeartbeatReply{0, false}
			rf.sendAppendEntries(i, args, reply)
			if reply.Success {
				cnt++
				rf.nextIndex[i]++
			}
		}

		if cnt > len(rf.peers)/2 {
			rf.commitIndex = log.Index
			// send commit command
			for i := 0; i < len(rf.peers); i++ {
				rf.sendCommit(i, log.Index)
			}
		}
		index = log.Index
	}

	return index, term, isLeader
}

//
// the tester calls Kill() when a Raft instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (rf *Raft) Kill() {
	fmt.Println("try to kill " + strconv.Itoa(rf.me))
	if rf.identity == Leader {
		rf.heartbeatQuitCh <- true
	}
	rf.leaderElectionQuitCh <- true
}

func GenLeaderElectionTimeout() int {
	return 300 + rand.Intn(300)
}

func LeaderElectionFunc(rf *Raft) {
	rf.electionTimeout = GenLeaderElectionTimeout()
	for {
		select {
		case <-rf.leaderElectionQuitCh:
			fmt.Println("quit leader election")
			return
		default:
			time.Sleep(10 * time.Millisecond)
			if rf.identity != Leader {
				//fmt.Println("node:" + strconv.Itoa(rf.me) + " timeout:" + strconv.Itoa(rf.electionTimeout))
				rf.electionTimeout -= 10
			} else {
				//fmt.Println("leader " + strconv.Itoa(rf.me) + " didn't decrement")
			}
			if rf.electionTimeout <= 0 {
				fmt.Println(strconv.Itoa(rf.me) + " timeout, start election! candidate " + strconv.Itoa(rf.me))
				rf.identity = Candidate
				voteCnt := 0
				rf.curTerm++
				args := &RequestVoteArgs{rf.me, rf.curTerm}
				//TODO: follower -> candidate
				for i := 0; i < len(rf.peers); i++ {
					if i == rf.me {
						voteCnt++
						continue
					}
					reply := &RequestVoteReply{0, 0, false}

					rf.sendRequestVote(i, args, reply)
					if reply.IsAccept {
						voteCnt++
					}
				}

				if voteCnt > len(rf.peers)/2 {
					fmt.Println(strconv.Itoa(rf.me) + " win! term: " + strconv.Itoa(rf.curTerm))
					rf.identity = Leader
					go HeartbeatFunc(rf)
				} else {
					fmt.Println(strconv.Itoa(rf.me) + " lose! term: " + strconv.Itoa(rf.curTerm))
					rf.identity = Follower
				}
				rf.electionTimeout = GenLeaderElectionTimeout()

			}
		}
	}
}

func HeartbeatFunc(rf *Raft) {
	for {
		select {
		case <-rf.heartbeatQuitCh:
			fmt.Println("node:" + strconv.Itoa(rf.me) + " stop heartbeat")
			return
		default:
			//fmt.Println(strconv.Itoa(rf.me) + " is sending heartbeat")
			time.Sleep(120 * time.Millisecond)
			for i := 0; i < len(rf.peers); i++ {
				args := &HeartbeatArgs{rf.curTerm, rf.me, 0, 0, nil, 0}
				reply := &HeartbeatReply{}
				rf.sendHeartbeat(i, args, reply)
			}
		}
	}
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

func DoApplyMsg(rf *Raft) {
	for {
		select {
		case <-rf.applyQuitCh:
			fmt.Println("quit do apply")
			return
		default:
			time.Sleep(1 * time.Millisecond)
			for rf.commitIndex > rf.lastApplied {
				fmt.Println(strconv.Itoa(rf.me) + " apply:" + strconv.Itoa(rf.lastApplied+1))
				fmt.Println(strconv.Itoa(rf.me) + " len:" + strconv.Itoa(len(rf.log)))
				log := rf.log[rf.lastApplied+1]
				applyMsg := ApplyMsg{true, log.Commnad, log.Index}
				rf.applyCh <- applyMsg
				rf.lastApplied++
			}
		}
	}
}

func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	rf.curTerm = 0
	rf.votedFor = -1
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.identity = Follower

	foo := Log{0, 0, 0}
	rf.log = append(rf.log, foo)

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// init background goroutine
	rf.electionTimeout = 0
	rf.heartbeatQuitCh = make(chan bool)
	rf.leaderElectionQuitCh = make(chan bool)
	rf.applyQuitCh = make(chan bool)
	rf.applyCh = applyCh
	go LeaderElectionFunc(rf)
	go DoApplyMsg(rf)
	return rf
}
