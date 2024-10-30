#python3 dstest -r --iter 1000 --workers 10 --timeout 40 --output out.log TestInitialElection3A TestReElection3A TestManyElections3A
#python3 dstest -r --iter 1000 --workers 10 --timeout 45 --output out.log TestBasicAgree3B TestRPCBytes3B TestFollowerFailure3B TestLeaderFailure3B TestFailAgree3B TestFailNoAgree3B TestConcurrentStarts3B TestRejoin3B TestBackup3B TestCount3B
#python3 dstest -r --iter 500 --workers 16 --timeout 120 --output out.log TestPersist13C TestPersist23C TestPersist33C TestFigure83C TestUnreliableAgree3C TestFigure8Unreliable3C TestReliableChurn3C TestUnreliableChurn3C TestSnapshotBasic3D TestSnapshotInstall3D TestSnapshotInstallUnreliable3D TestSnapshotInstallCrash3D TestSnapshotInstallUnCrash3D TestSnapshotAllCrash3D TestSnapshotInit3D
#python3 dstest -r --iter 500 --workers 10 --timeout 120 --output out.log TestSnapshotBasic3D TestSnapshotInstall3D TestSnapshotInstallUnreliable3D TestSnapshotInstallCrash3D TestSnapshotInstallUnCrash3D TestSnapshotAllCrash3D TestSnapshotInit3D
# python3 dstest --iter 500 --workers 10 --timeout 120 --output ser.log TestManyElections3A TestBackup3B TestFigure8Unreliable3C TestUnreliableChurn3C TestSnapshotInstallUnreliable3D TestSnapshotInstallUnCrash3D TestSnapshotAllCrash3D TestSnapshotInit3D


#python3 dstest --iter 500 --workers 16 --timeout 120 --output out.log84 TestSnapshotInstallUnreliable3D
python3 dstest --iter 32 --workers 16 --timeout 60 --output bug TestSnapshotUnreliable4B