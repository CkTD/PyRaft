all_ports="8000 8001 8002"

rm -f pids *.log *.state
for port in ${all_ports}; do
    python3 raft_node.py --log_file=./${port}.log --raft_log_file=./${port}.raft ${port} ${all_ports/${port}/} & 
    echo $! >> ./pids
done
