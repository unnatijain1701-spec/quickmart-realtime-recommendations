#!/bin/bash
# Run this INSIDE Ubuntu (WSL) on the new laptop. It installs Java and Kafka, prepares storage,
# and writes ~/start_kafka.sh (the demo script uses it). Safe to run twice.
set -e
echo "1/4 Installing Java (it will ask for your Ubuntu password; typing shows nothing)..."
sudo apt-get update -y
sudo apt-get install -y openjdk-17-jre-headless wget
cd ~
if [ ! -d kafka_2.13-3.9.0 ]; then
  echo "2/4 Downloading Kafka (about 120 MB)..."
  wget -q --show-progress https://archive.apache.org/dist/kafka/3.9.0/kafka_2.13-3.9.0.tgz || wget -q --show-progress https://dlcdn.apache.org/kafka/3.9.0/kafka_2.13-3.9.0.tgz
  tar -xzf kafka_2.13-3.9.0.tgz
else
  echo "2/4 Kafka is already downloaded."
fi
cd kafka_2.13-3.9.0
echo "3/4 Preparing Kafka storage..."
sed -i "s|^log.dirs=.*|log.dirs=$HOME/kafka-data|" config/kraft/server.properties
ID=$(bin/kafka-storage.sh random-uuid)
bin/kafka-storage.sh format --ignore-formatted -t "$ID" -c config/kraft/server.properties
echo "4/4 Writing ~/start_kafka.sh ..."
cat > ~/start_kafka.sh <<'EOS'
#!/bin/bash
cd ~/kafka_2.13-3.9.0
export KAFKA_HEAP_OPTS="-Xmx512m -Xms512m"
exec bin/kafka-server-start.sh config/kraft/server.properties
EOS
chmod +x ~/start_kafka.sh
echo ""
echo "DONE. Kafka is installed. You can close this window."
