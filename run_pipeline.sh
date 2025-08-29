#!/bin/bash
set -e  # agar koi error aata hai to script yahin ruk jaye

# Date-time ka naam banayein taake har run ka alag log bane
LOG_FILE="logs/pipeline_$(date +'%Y%m%d_%H%M%S').log"

echo "Pipeline started at $(date)" | tee -a $LOG_FILE

# Step 1: Extract
python3 scripts/extract.py 2>&1 | tee -a $LOG_FILE

# Step 2: Transform
python3 scripts/transform.py 2>&1 | tee -a $LOG_FILE

# Step 3: Load
python3 scripts/load.py 2>&1 | tee -a $LOG_FILE

# Step 4: Visualize
python3 scripts/visualize.py 2>&1 | tee -a $LOG_FILE

echo "Pipeline finished at $(date)" | tee -a $LOG_FILE
