#!/bin/bash
# create-topics.sh (STUB, comments only)
# - wait for the broker to be ready
# - create every topic listed in kafka/topics.md with --partitions and --replication-factor
# - create matching .dlq topics
# - use --if-not-exists so restarts are safe
# - list topics at the end for verification
