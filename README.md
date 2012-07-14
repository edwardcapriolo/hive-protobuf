hive-protobuf
=============

Protobuf input format and Serde supportThe input format needs to be on the hive auxpath for testing but currently the aux path can not be adjusted at runtime. Will add this
capability to hive_test soon.

Do these steps to run tests

    jar -cf a.jar com/m6d/hive/protobuf/KVAsVSeqFileBinaryInputFormat.class com/m6d/hive/protobuf/KVAsVSeqFileBinaryInputFormat\$KVAsVSeqFileBinaryRecordReader.class com/m6d/hive/protobuf/Pair.class

    mv a.jar /home/edward/hadoop/hadoop-0.20.2_local/lib/
    cp .m2/repository/protobuf-2.4.1.jar /home/edward/hadoop/hadoop-0.20.2_local/lib/

