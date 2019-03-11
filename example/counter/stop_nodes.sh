for pid in $(cat pids); do
    kill $pid
done
