#!/bin/bash
rsync -avz \
  --exclude='.venv' --exclude='venv' \
  --exclude='__pycache__' --exclude='*.pyc' \
  --exclude='logs/' \
  --exclude='*.db' --exclude='*.sqlite' --exclude='*.sqlite3' \
  --exclude='data/' \
  /home/kevi/polymarket-bot/ \
  root@178.105.56.171:/root/polymarket-bot/
ssh root@178.105.56.171 "systemctl restart polymarket-bot"
echo "Deployed and restarted."
