# capDB
Distributed KV Database 


### M1
This milestone focuses on building a single node DB server with a CLI.
Implemented marshalling and communication protocol to support GET/PUT in KV DB.
Implemented retry, heartbeat and accounted for transmission error in all communication code.
Developed storage solution for storage KV.

Design doc - https://docs.google.com/document/d/1gwvO7dwep4TxjnzfGOD2t4eOm1OoH_p6W2yXfq_2ppg/edit#heading=h.l0pom9xggwwx

### M2
This milestone focused on scaling single node to cluster of node DBs coordinated by zookeeper.
Implemented server managemente system via ECS which gave a cli for scaling up/down or interacting with the data.
Designed event driven system which coordinated all nodes via Zookeeper. 

Design doc - https://docs.google.com/document/d/1ohmL8vpTKRWngoFawFT9bjTub6jpeLqJCdWVOja-Bfs/edit#heading=h.5og4tznnlsi9

### M3
This milestone focused on adding replication strategy to implement eventual consistency in the system.
Implement replication logic to have one primary and 2 secondary nodes which are allowed to serve reads.
Added consistent hashing to distribute load across all nodes.
Use Zookeper to provide failure detection and support process of recaculating hash ring in metadata for all nodes.

Design doc - https://docs.google.com/document/d/14oYz41Dm2_xAv-2X9FfpSRNt588wZZ_xn1BYm-bHDp4/edit#heading=h.atqd1cj05pgn

### M4
This milestone focused on creative design - we implemeted auto-scaling to make our DB Distributed system resiliant.

Design doc - https://docs.google.com/document/d/1a4xZPFVu9V0K5mWzO2vqEeq-hAkqpO6Yk40YWNY7o88/edit?usp=sharing
Final project slides - https://docs.google.com/presentation/d/1fwYh28ZOxVYp4Buq-KK1sLD_njIXi_eykYYutPlTVT8/edit?usp=sharing
