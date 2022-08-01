echo "" > raftA.log
int=1
while (($int<=20))
do
    echo $int >> raftA.log
    go test -run 2A >> raftA.log
    # go test -run TestInitialElection2A >> raftA.log
    # go test -run TestReElection2A >> raftA.log
    # go test -run TestManyElections2A >> raftA.log

    # go test -run TestBasicAgree2B > raftB.log
    # go test -run TestRPCBytes2B > raftB.log
    # go test -run For2023TestFollowerFailure2B > raftB.log
    # go test -run For2023TestLeaderFailure2B > raftB.log

    # go test -run TestFailAgree2B > raftB.log
    # go test -run TestFailNoAgree2B > raftB.log
    # go test -run TestConcurrentStarts2B > raftB.log
    # go test -run TestRejoin2B > raftB.log
    # go test -run TestBackup2B > raftB.log
    # go test -run TestCount2B > raftB.log

    # go test -run 2C > raftC.log


    # go test -run 2D > raftD.log
    echo "" >> raftA.log
    let "int++"
done