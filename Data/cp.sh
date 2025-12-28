

set -e 

echo "=== here we will copy all the data from the data directory to the producer container ==="

docker exec -it producer bash -c '
    cd ..
    mkdir -p Data
    exit '

docker cp ./datasets_json producer:/Data/