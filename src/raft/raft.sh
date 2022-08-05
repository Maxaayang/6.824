log="raftB.log"

# echo "" > raftA.log
echo "" > raftB.log
# echo "" > $log
# echo "" > raftC.log
# echo "" > raftD.log
int=1
while (($int<=1))
do
    echo "loop "$int
    echo "loop "$int >> raftB.log
    # go test -run 2A >> $log
    # go test -run TestInitialElection2A >> $log
    # go test -run TestReElection2A >> $log
    # go test -run TestManyElections2A >> $log

    # echo "loop "$int >> raftB.log
    # go test -run TestBasicAgree2B >> raftB.log
    # go test -run TestRPCBytes2B >> raftB.log
    # go test -run For2023TestFollowerFailure2B >> raftB.log
    # go test -run For2023TestLeaderFailure2B >> raftB.log
    # go test -run TestFailAgree2B >> raftB.log


    # go test -run TestFailNoAgree2B >> raftB.log
    # 50次不出错
    # go test -run TestConcurrentStarts2B >> raftB.log
    # go test -run TestRejoin2B >> raftB.log

    # go test -run TestBackup2B >> raftB.log
    go test -run TestCount2B >> raftB.log

    # go test -run 2C >> $log
    
    # go test -run TestPersist12C >> $log
    # go test -run TestPersist22C >> $log
    # go test -run TestPersist32C >> $log
    # go test -run TestFigure82C >> $log
    # go test -run TestUnreliableAgree2C >> $log
    # go test -run TestFigure8Unreliable2C >> $log
    # go test -run TestReliableChurn2C >> $log
    # go test -run TestUnreliableChurn2C >> $log

    # go test -run 2D >> $log
    # go test -run TestSnapshotBasic2D >> $log
    # go test -run TestSnapshotInstall2D >> $log
    # go test -run TestSnapshotInstallUnreliable2D >> $log
    # go test -run TestSnapshotInstallCrash2D >> $log
    # go test -run TestSnapshotInstallUnCrash2D >> $log
    # go test -run TestSnapshotAllCrash2D >> $log

    echo "" >> raftB.log
    let "int++"
done