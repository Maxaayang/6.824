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
	"fmt"
	"log"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	// "fmt"

	//	"6.824/labgob"
	"6.824/labrpc"
)

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
//

// var electionTimeout = time.Duration(200) * time.Millisecond	// 心跳超时时间

// type State []byte

const heartTime = 100 // 定期发送心跳的时间

const (
	follower    = 1
	candidate = 2
	leader  = 3
)

type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

type LogEntry struct {
	// Command string
	Command interface{}
	Term    int
	Index   int
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	log []LogEntry

	currentTerm  int // 当前的任期
	currentState int // 服务器当前的状态
	votedFor     int // 在当前任期内投票给了谁
	voteCount    int // 当前获取到的选票的数目

	commitIndex   int       // 最后一次提交的log的位置
	lastApplied   int       // 最近一次的log
	lastLogTerm   int       // 最近一次log的term
	lastHeartBeat time.Time // 最近一次收到 HeartBeat 的时间

	nextIndex  []int // 每个服务器需要发送给它的下一条日志的索引(初始化为leader最后一条日志索引+1)
	matchIndex []int // 每个服务器已复制给它的日志的最高索引值

	leader int // 当前leader的位置

	applyCh chan ApplyMsg

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	term := rf.currentTerm
	isleader := rf.leader == rf.me
	// Your code here (2A).
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
// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
//
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {

	// Your code here (2D).

	return true
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).

}

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int // candidate当前的term
	CondidateId  int // candidate的ID
	LastLogIndex int // candidate上一条日志的Index
	LastLogTerm  int // candidate上一条日志的term
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	// 如果term < currentTerm 返回 currentTerm，否则返回term
	Term           int
	IntvoteGranted bool
}

func (rf *Raft) agreeVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// log.Printf("follower %d 在term %d 同意给 %d 投票", rf.me, rf.currentTerm, args.CondidateId)
	rf.votedFor = args.CondidateId
	rf.currentTerm = args.Term
	reply.IntvoteGranted = true
	reply.Term = rf.currentTerm
	log.Printf("server %d aggre vote to server %d at term %d, server lastApplied %d, lastLogTerm %d", rf.me, args.CondidateId, rf.currentTerm, rf.lastApplied, rf.lastLogTerm)
}

func (rf *Raft) disagreeVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// log.Printf("follower %d 在term %d 拒绝给 %d 投票", rf.me, rf.currentTerm, args.CondidateId)
	reply.IntvoteGranted = false
	reply.Term = rf.currentTerm
	// log.Printf("server %d disaggre vote to server %d at term %d, server lastApplied %d, lastLogTerm %d", rf.me, args.CondidateId, rf.currentTerm, rf.lastApplied, rf.lastLogTerm)
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	// 一个任期只能投一票
	// 如果任期小于当前的任期，直接否决
	// TODO 在候选者任期比自己大的情况下还的比较日志的新旧程度
	rf.mu.Lock()
	// defer rf.mu.Unlock()
	if args.Term < rf.currentTerm {
		rf.disagreeVote(args, reply)
		log.Printf("server %d 拒绝在term %d 给server %d 投票, 因为term太小", rf.me, rf.currentTerm, args.CondidateId)
		rf.mu.Unlock()
		return
	} else {
		if args.Term == rf.currentTerm && rf.votedFor != -1 {
			rf.disagreeVote(args, reply)
			log.Printf("server %d 拒绝在term %d 给server %d 投票, 因为已经投给了 %d", rf.me, rf.currentTerm, args.CondidateId, rf.votedFor)
			rf.mu.Unlock()
			return
		}
		rf.votedFor = -1
		rf.currentTerm = args.Term
		if rf.lastLogTerm > args.LastLogTerm {
			rf.disagreeVote(args, reply)
			log.Printf("server %d 拒绝在term %d 给server %d 投票, 因为lastLogTerm太小", rf.me, rf.currentTerm, args.CondidateId)
			rf.mu.Unlock()
			return
		} else if rf.lastLogTerm == args.LastLogTerm && rf.lastApplied > args.LastLogIndex {
			rf.disagreeVote(args, reply)
			log.Printf("server %d 拒绝在term %d 给server %d 投票, 因为lastApplied太小", rf.me, rf.currentTerm, args.CondidateId)
			rf.mu.Unlock()
			return
		}


		rf.agreeVote(args, reply)
		rf.mu.Unlock()
		return
	}
	// } else if rf.votedFor == -1 {
	// 	if args.LastLogTerm > rf.lastLogTerm {
	// 		rf.agreeVote(args, reply)
	// 		return
	// 	} else if args.LastLogTerm < rf.lastLogTerm {
	// 		rf.disagreeVote(args, reply)
	// 		return
	// 	} else {
	// 		if args.LastLogIndex < rf.lastApplied {
	// 			rf.disagreeVote(args, reply)
	// 			return
	// 		} else {
	// 			rf.agreeVote(args, reply)
	// 			return
	// 		}
	// 	}
	// }
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
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
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
	// Your code here (2B).
	rf.mu.Lock()
	// defer rf.mu.Unlock()
	if rf.leader == rf.me {
		rf.lastHeartBeat = time.Now()
		index := rf.lastApplied + 1
		//if len(rf.log) == 0 {
		//	index = 0
		//} else {
		//	index = rf.log[len(rf.log)-1].Index + 1
		//}

		appendLog := LogEntry{Command: command, Term: rf.currentTerm, Index: index}
		log.Printf("log is %v", appendLog)
		rf.log = append(rf.log, appendLog)
		rf.matchIndex[rf.me] = index
		rf.nextIndex[rf.me] = index + 1
		args := AppendEntriesArgs{false, rf.currentTerm, rf.me, rf.lastApplied, rf.lastLogTerm, rf.log[len(rf.log) - 1 :]}
		rf.lastApplied = index
		rf.lastLogTerm = rf.currentTerm
		rf.mu.Unlock()

		// fmt.Println("entries args is", args)

		// rf.sendAppendEntries(args)
		// TODO

		rf.sendAppendEntries(args)
		log.Printf("leader %d logs are %v", rf.me, rf.log)
		// log.Printf("leader %d send prevLogIndex is %d , the prevLogTerm is %d", rf.me, rf.lastApplied, rf.lastLogTerm)

		return index, rf.currentTerm, rf.leader == rf.me

	}

	rf.mu.Unlock()
	return -1, -1, rf.leader == rf.me
}

//
// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
//
func (rf *Raft) Kill() {
	if !rf.killed() {
		atomic.StoreInt32(&rf.dead, 1)
	}
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) ticker() {
	for !rf.killed() {
		// Your code here to check if a leader election should
		// be started and to randomize sleeping time using
		// time.Sleep().
		// 先sleep一段时间，然后判断上一次收到心跳的时间是否超时
		// TODO 这里的过期时间是要随机的
		lastHeartBeat := rf.getLastHeartBeat()
		i := 250 + rand.Intn(150)
		// fmt.Println("sleep: ", i)
		time.Sleep(time.Duration(i) * time.Millisecond)
		if lastHeartBeat != rf.getLastHeartBeat() {
			continue
		}
		rf.election()

	}
}

func (rf *Raft) election() {
	rf.mu.Lock()
	// defer rf.mu.Unlock()
	rf.changeState(candidate)
	rf.voteCount++
	// fmt.Println("voteCount: ", rf.voteCount)
	rf.votedFor = rf.me
	rf.leader = -1	// 这里一定要变成 -1, 否则会误认为自己还是leader
	rf.currentTerm++
	args := RequestVoteArgs{rf.currentTerm, rf.me, rf.lastApplied, rf.lastLogTerm}
	log.Printf("server %d 在term %d 请求投票, lastApplied %d, lastLogTerm %d", rf.me, rf.currentTerm, rf.lastApplied, rf.lastLogTerm)
	rf.mu.Unlock()
	go rf.checkState() // 启动一个线程检查一定时间之后是否赢得选举
	// fmt.Println("peers:", len(rf.peers))
	for serverNum := 0; serverNum < len(rf.peers); serverNum++ {
		if serverNum != rf.me {
			// fmt.Println(i)
			go func(server int, args RequestVoteArgs) {
				reply := RequestVoteReply{}
				if rf.sendRequestVote(server, &args, &reply) {
					rf.handleRequse(reply)
				}
			}(serverNum, args)
		}
	}

}

func (rf *Raft) changeState(state int) {
	// rf.mu.Lock()
	// defer rf.mu.Unlock()
	// log.Printf("server %d change state at term %d, %d -> %d", rf.me, rf.currentTerm, rf.currentState, state)
	rf.currentState = state
	// log.Printf("server %d change state to %d at term %d", rf.me, state, rf.currentTerm)
}

func (rf *Raft) getLastHeartBeat() time.Time {
	return rf.lastHeartBeat
}

// 到达时间之后还未成为leader的话，直接变为follower
func (rf *Raft) checkState() {
	time.Sleep(time.Duration(250+rand.Intn(50)) * time.Millisecond)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.currentState != leader {
		log.Printf("server %d 在term %d 因为超时竞选失败, 变为follower", rf.me, rf.currentTerm)
		rf.changeState(follower)
		rf.leader = -1
		rf.voteCount = 0
		rf.votedFor = -1
	}
}

func (rf *Raft) sendHeartBeat() {
	args := AppendEntriesArgs{Vote: true, Term: rf.currentTerm, LeaderId: rf.me, PrevLogIndex: rf.commitIndex, PrevLogTerm: rf.currentTerm}
	for {
		time.Sleep(heartTime * time.Microsecond)
		if !rf.killed() && rf.currentState == leader {
			if time.Since(rf.getLastHeartBeat()) > time.Duration(heartTime) * time.Microsecond {
				rf.sendAppendEntries(args)
			}
		} else {
			break
		}
	}
}

func (rf *Raft) handleRequse(reply RequestVoteReply) {
	rf.mu.Lock()
	// defer rf.mu.Unlock()
	// log.Printf("server %d 的voteCount: %d", rf.me, rf.voteCount)
	if rf.voteCount == 0 {
		log.Printf("server %d 在term %d 当前的票数为 %d", rf.me, rf.currentTerm, rf.voteCount)
		// fmt.Println("rf.voteCount == 0")
		rf.mu.Unlock()
		return
	}
	if reply.Term > rf.currentTerm {
		log.Printf("server %d 在term %d 因为term较小竞选失败, 变为follower", rf.me, rf.currentTerm)
		rf.currentTerm = reply.Term
		rf.changeState(follower)
		// fmt.Println("rf.currentTerm < reply.Term")
		rf.voteCount = 0
		rf.votedFor = -1
		rf.mu.Unlock()
	} else if reply.IntvoteGranted && rf.currentState == candidate {
		rf.voteCount++
		rf.mu.Unlock()
		log.Printf("server %d 在term %d 当前的票数为 %d", rf.me, rf.currentTerm, rf.voteCount)
		// fmt.Println("voteCount: ", rf.voteCount)
		// 如果已经达到了多数，就直接变为leader, 并将票数归零
		if rf.voteCount > (len(rf.peers)>>1) && rf.currentState == candidate {
			log.Printf("server %d 在term %d 当选为leader, 得票为 %d, 总数为 %d", rf.me, rf.currentTerm, rf.voteCount, len(rf.peers))
			rf.mu.Lock()
			rf.changeState(leader)
			rf.leader = rf.me
			rf.voteCount = 0
			rf.votedFor = -1
			// args := AppendEntriesArgs{true, rf.currentTerm, rf.me, rf.commitIndex, rf.currentTerm, LogEntry{}}
			args := AppendEntriesArgs{Vote: true, Term: rf.currentTerm, LeaderId: rf.me, PrevLogIndex: rf.commitIndex, PrevLogTerm: rf.currentTerm}
			// reply := AppendEntriesReply
			rf.mu.Unlock()
			rf.sendAppendEntries(args)
			// 开始定期给follower发送心跳
			go rf.sendHeartBeat()
		}
	} else {
		rf.mu.Unlock()
		log.Printf("server %d 在term %d 竞选出bug, 当前票数为 %d", rf.me, rf.currentTerm, rf.voteCount)
	}
}

type AppendEntriesArgs struct {
	// Your data here (2A).
	// 如果term < currentTerm 返回 currentTerm，否则返回term
	Vote         bool // 是否是心跳或者当选之后的信息
	Term         int  // leader的任期
	LeaderId     int  // leader的ID
	PrevLogIndex int  // 追加日志的其实Index
	PrevLogTerm  int  // 追加日志的任期
	Entries      []LogEntry
	// LeaderCommit bool
}

type AppendEntriesReply struct {
	// Your data here (2A).
	// 如果term < currentTerm 返回 currentTerm，否则返回term
	Term    int
	Success bool
	LastIndex int
	LastTrem int
}

type LogCommitArgs struct {
	Commit bool
	Index int
}

type LogCommitReply struct {
	Commit bool
}

func (rf *Raft) checkCommit(commits *int) {
	// rf.mu.Lock()
	// defer rf.mu.Unlock()
	for {
		if *commits >= len(rf.peers) >> 1 {
			args := LogCommitArgs{Commit: true, Index: rf.lastApplied}
			for server := range rf.peers {
				go func(server int, args *LogCommitArgs) {
					reply := LogCommitReply{}
					for {
						rf.peers[server].Call("Raft.CommitLog", args, &reply)
						// log.Printf("call server %d to commit", server)
						if reply.Commit {
							break
						}
					}
				}(server, &args)
			}
			break
		}
	}
}

func (rf *Raft) CommitLog(args *LogCommitArgs, reply *LogCommitReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// log.Printf("server %d receive commit", rf.me)
	if args.Commit {
		for {
			// rf.mu.Lock()
			// defer rf.mu.Unlock()
			if rf.commitIndex < args.Index && len(rf.log) > args.Index {
				// log.Printf("server %d logs lengh %d, commitIndex %d", rf.me, len(rf.log), rf.commitIndex)
				// rf.commitIndex++
				// log.Printf("server %d log length is %d, commit index is %d, args index is %d", rf.me, len(rf.log), rf.commitIndex, args.Index)
				// TODO out of range
				msg := ApplyMsg{CommandValid: true, Command: rf.log[rf.commitIndex + 1].Command, CommandIndex: rf.log[rf.commitIndex + 1].Index}
				rf.applyCh <- msg
				rf.commitIndex++
				// log.Printf("%d 在 term %d 往上层状态机成功发送 Msg %+v", rf.me, rf.currentTerm, msg)
				// log.Printf("server %d commit %v at term %d", rf.me, msg, rf.currentTerm)
				if rf.commitIndex == args.Index {
					reply.Commit = true
				}
			} else {
				break
			}
		}
	}
}

// TODO 给每个服务器发送log是要leader自己控制还是交给此函数来控制？
func (rf *Raft) sendAppendEntries(args AppendEntriesArgs) bool {
	rf.mu.Lock()
	// defer rf.mu.Unlock()
	// TODO 在这里判断一下是否需要发送log
	if args.Vote {
		for server := range rf.peers {
			if server != rf.me {
				go func(server int, args AppendEntriesArgs) {
					// log.Printf("server %d send heart to follower %d at term %d", rf.me, server, rf.currentTerm)
					reply := AppendEntriesReply{}
					rf.peers[server].Call("Raft.AppendEntries", &args, &reply)
					if reply.Term > rf.currentTerm {
						rf.changeState(follower)
					}
				}(server, args)
			}
		}
		rf.mu.Unlock()
		return true
	}

	commits := 0
	// log.Printf("commits: %d", commits)
	for server := range rf.peers {
		if server != rf.me {
			// log.Printf("server is: %d", server)
			go func(server int, args AppendEntriesArgs, commits *int) {
				// log.Printf("server is: %d", server)
				// rf.matchIndex[server] = args.PrevLogIndex
				// log.Printf("PrevLogIndex: %d", args.PrevLogIndex)
				// rf.nextIndex[server] = args.Entries.Index
				length := len(rf.log)
				logLength := len(rf.log)
				reply := AppendEntriesReply{}
				// log.Printf("leader %d server %d at term %d matchIndex %d %d, nextIndex %d %d", rf.me, server, rf.currentTerm, rf.matchIndex[server], args.PrevLogIndex, rf.nextIndex[server], args.Entries.Index)
				Loop1:
				// TODO
				// for {
				// 	if rf.matchIndex[server] == args.PrevLogIndex && rf.nextIndex[server] == args.Entries.Index {
						for {
							if rf.leader == rf.me {
								log.Printf("leader %d send server %d args: %v at term %d", rf.me, server, args, rf.currentTerm)
								rf.peers[server].Call("Raft.AppendEntries", &args, &reply)
								// log.Printf("call %d appendentries", server)
								// 这里得区分是不是复制的同一个日志
								// if reply.Success && length == logLength{

								// } else if reply.Success {

								if !reply.Success && reply.Term > rf.currentTerm {
									log.Printf("server %d 退下leader", rf.me)
									rf.currentTerm = reply.Term
									rf.changeState(follower)
								} else if !reply.Success && reply.LastIndex > args.PrevLogIndex {
									log.Printf("Loop1 reply.LastIndex %d, args.PrevLogIndex %d", reply.LastIndex, args.PrevLogIndex)
									break Loop1
								} else if !reply.Success && length > 2 && reply.Term != 0 {
									log.Printf("server %d matchIndex后移", server)
									if rf.log[reply.LastIndex].Index == reply.LastIndex && rf.log[reply.LastIndex].Term == reply.LastTrem {
										args.Entries = rf.log[reply.LastIndex + 1:]
										args.PrevLogIndex = rf.log[reply.LastIndex].Index
										args.PrevLogTerm = rf.log[reply.LastIndex].Term
									} else {
										// rf.matchIndex[server]--
										// rf.nextIndex[server]--
										args.Entries = rf.log[length - 2:length - 1]
										args.PrevLogIndex = rf.log[length-3].Index
										args.PrevLogTerm = rf.log[length-3].Term
										length--
									}
									args.Term = rf.currentTerm
								} else if reply.Success && length < logLength {
									log.Printf("server %d matchIndex前移", server)
									// length++
									// rf.matchIndex[server]++
									// rf.nextIndex[server]++
									// args.Entries = rf.log[length - 1]
									args.Entries = rf.log[length - 1 :]
									args.PrevLogIndex = rf.log[length - 2].Index
									args.PrevLogTerm = rf.log[length - 2].Term
									args.Term = rf.currentTerm
									length += len(args.Entries)
									
									// length++
								} else if reply.Success && length == logLength {
									rf.matchIndex[server] = args.Entries[len(args.Entries) - 1].Index
									rf.nextIndex[server] = args.Entries[len(args.Entries) - 1].Index + 1
									log.Printf("server %d commit成功, commitIndex is %d matchIndex %d, nextIndex %d", server, length, rf.matchIndex[server], rf.nextIndex[server])
									// commits++
									*commits++
									break Loop1
								}
							} else {
								break Loop1
							}
						}
					// }
				// }
			}(server, args, &commits)
		}
	}
	rf.mu.Unlock()
	go rf.checkCommit(&commits)
	return true
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	// defer rf.mu.Unlock()
	if args.Vote {
		if rf.currentTerm > args.Term {
			reply.Term = rf.currentTerm
			reply.Success = false
			rf.mu.Unlock()
			return
		} else {
			// log.Printf("follower %d recive heart from leader %d at term %d", rf.me, args.LeaderId, rf.currentTerm)
			rf.lastHeartBeat = time.Now() // 更新leader心跳的时间
			rf.leader = args.LeaderId     // 更新leader的ID
			rf.votedFor = -1
			rf.voteCount = 0
			rf.changeState(follower)
			rf.currentTerm = args.Term
			reply.Term = rf.currentTerm
			reply.Success = true
			rf.mu.Unlock()
			return
		}
	}
	log.Printf("server %d receive args %v from leader %d at term %d", rf.me, args, args.LeaderId, rf.currentTerm)
	if args.Term < rf.currentTerm {
		log.Printf("rf term: %d > args term: %d", rf.currentTerm, args.Term)
		reply.Term = rf.currentTerm
		reply.Success = false
		rf.mu.Unlock()
		return
	// } else if args.PrevLogIndex > rf.lastApplied || args.PrevLogTerm > rf.lastLogTerm {
	} else if args.PrevLogIndex > rf.lastApplied {
		log.Printf("server %d args.PrevLogIndex: %d, rf.lastApplied: %d, args.PrevLogTerm: %d, rf.lastLogTerm: %d", rf.me, args.PrevLogIndex, rf.lastApplied, args.PrevLogTerm, rf.lastLogTerm)
		reply.Term = rf.currentTerm
		reply.Success = false
		reply.LastIndex = rf.lastApplied
		reply.LastTrem = rf.lastLogTerm
		rf.mu.Unlock()
		return
	} else {
		// TODO 这里如果leader的log短了的话需要进行覆盖, 并且把多余的进行删除
		length := len(args.Entries)
		if args.PrevLogIndex < rf.lastApplied && rf.lastLogTerm < args.Entries[0].Term {
			rf.log = rf.log[:args.PrevLogIndex + 1]
			log.Printf("server %d logs is %v", rf.me, rf.log)
			rf.lastApplied = rf.log[len(rf.log) - 1].Index
			rf.lastLogTerm = rf.log[len(rf.log) - 1].Term
		}
		if rf.lastApplied == args.PrevLogIndex && rf.lastLogTerm == args.PrevLogTerm {
			log.Printf("args -> rf")
			rf.lastApplied = args.Entries[length - 1].Index
			rf.lastLogTerm = args.Entries[length - 1].Term
			rf.log = append(rf.log, args.Entries...)
			rf.currentTerm = args.Term
			reply.Term = rf.currentTerm
			reply.Success = true
			log.Printf("server %d logs are %v", rf.me, rf.log)
			rf.mu.Unlock()
			return
		}
		log.Printf("args.PrevLogIndex: %d, rf.lastApplied: %d, args.PrevLogTerm: %d, rf.lastLogTerm: %d", args.PrevLogIndex, rf.lastApplied, args.PrevLogTerm, rf.lastLogTerm)
		reply.Success = false
		reply.Term = rf.currentTerm
		reply.LastIndex = rf.lastApplied
		reply.LastTrem = rf.lastLogTerm
		rf.mu.Unlock()
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
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	rf.currentState = follower
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.votedFor = -1
	rf.lastLogTerm = 0
	rf.applyCh = applyCh
	rf.matchIndex = make([]int, len(peers))
	rf.nextIndex = make([]int, len(peers))
	rf.log = make([]LogEntry, 1)
	rf.lastHeartBeat = time.Now()

	for index := range peers {
		rf.matchIndex[index] = 0
		rf.nextIndex[index] = 1
	}

	// Your initialization code here (2A, 2B, 2C).

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	// 初始化之后立马进行选举
	// rf.election()
	go rf.ticker()
	fmt.Print()
	log.Print()

	return rf
}
