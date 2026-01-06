#!/bin/bash

# A script to reset the data in the Steam data Kafka topics by deleting and recreating them.
# This is useful for re-running the historical backfill from a clean state.

# --- CONFIGURATION ---
NAMESPACE="kafka"
CLUSTER_NAME="my-kafka-cluster"
TOPICS_TO_RESET=(
  "steam-games-raw"
  "steam-reviews-raw"
  "steam-player-counts-raw"
)

echo "--- Kafka Topic Reset Tool ---"
echo "This will DELETE ALL DATA in the specified topics."
read -p "Are you sure you want to continue? (y/n) " -n 1 -r
echo # Move to a new line

if [[ ! $REPLY =~ ^[Yy]$ ]]
then
    echo "Aborted."
    exit 1
fi

echo ""
echo "Deleting Kafka topics..."

for topic in "${TOPICS_TO_RESET[@]}"
do
  echo " - Deleting topic: $topic"
  kubectl delete kafkatopic "$topic" -n "$NAMESPACE" --ignore-not-found=true
done

# Wait for the topics to be fully deleted.
# Strimzi's operator will handle the actual deletion in the background.
echo ""
echo "Waiting for topics to be fully removed... (this may take up to 30 seconds)"
sleep 30

echo ""
echo "Recreating Kafka topics..."

# Now, we re-apply our original topic definitions.
# This assumes you have the 'kafka-topics.yaml' file in the same directory.
TOPIC_DEFINITION_FILE="kafka-topics.yaml"

if [ -f "$TOPIC_DEFINITION_FILE" ]; then
  kubectl apply -f "$TOPIC_DEFINITION_FILE" -n "$NAMESPACE"
  echo "Topics have been recreated."
  echo ""
  echo "Verifying topic status:"
  kubectl get kafkatopics -n "$NAMESPACE"
  echo ""
  echo "--- Reset Complete ---"
  echo "You can now run your bootstrap_kafka.py script again."
else
  echo "ERROR: The topic definition file '$TOPIC_DEFINITION_FILE' was not found."
  echo "Please place it in the same directory as this script."
fi