echo "" > kv.log
i=1

while (($i<=1)) {
    echo "loop" $i
    echo "loop" $i >> kv.log

    # go test -run 3A >> kv.log
    # go test -run TestBasic3A >> kv.log
    go test -run TestSpeed3A >> kv.log
    # go test -run TestConcurrent3A >> kv.log
    # go test -run TestUnreliable3A >> kv.log
    # go test -run TestUnreliableOneKey3A >> kv.log
    # go test -run TestOnePartition3A >> kv.log
    # go test -run TestManyPartitionsOneClient3A >> kv.log
    # go test -run TestManyPartitionsManyClients3A >> kv.log
    # go test -run TestPersistOneClient3A >> kv.log
    # go test -run TestPersistConcurrent3A >> kv.log
    # go test -run TestPersistConcurrentUnreliable3A >> kv.log
    # go test -run TestPersistPartition3A >> kv.log
    # go test -run TestPersistPartitionUnreliable3A >> kv.log
    # go test -run TestPersistPartitionUnreliableLinearizable3A >> kv.log

    # go test -run 3B >> kv.log
    # go test -run TestSnapshotRPC3B >> kv.log
    # go test -run TestSnapshotSize3B >> kv.log
    # go test -run TestSpeed3B >> kv.log
    # go test -run TestSnapshotRecover3B >> kv.log
    # go test -run TestSnapshotRecoverManyClients3B >> kv.log
    # go test -run TestSnapshotUnreliable3B >> kv.log
    # go test -run TestSnapshotUnreliableRecover3B >> kv.log
    # go test -run TestSnapshotUnreliableRecoverConcurrentPartition3B >> kv.log
    # go test -run TestSnapshotUnreliableRecoverConcurrentPartitionLinearizable3B >> kv.log

    echo "" >> kv.log
    let "i++"
}