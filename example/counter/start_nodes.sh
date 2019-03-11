all_ports="8000 8001 8002"

rm -f pids *.log
for port in ${all_ports}; do
    python3 raft_node.py --log_file=./${port}.log ${port} ${all_ports/${port}/} & 
    echo $! >> ./pids
done
