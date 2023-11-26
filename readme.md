# Cloud Computing : Distributed and Fault Tolerant Master
1. Here I used two apporoaches Redis-Raft (Distributed version of Redis) and BGSAVE of Redis to make fault tolerant server.
2. BGSAVE will create periodic checkpoints.
3. Redis-Raft will create a distributed cluster.   
3.1 To run BGSAVE, assign Is_raft = False
4. To run the Redis-Raft, Create a cluster node and connect redis client to it.
5. In this case, assign Is_raft = True.