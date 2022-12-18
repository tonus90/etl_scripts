#!/bin/bash

# Start the first process
airflow scheduler &
  
# Start the second process
airflow webserver --port 8080 &
  
# Wait for any process to exit
wait -n
  
# Exit with status of process that exited first
exit $?