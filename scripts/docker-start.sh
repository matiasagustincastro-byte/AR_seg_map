#!/bin/sh
set -eu

echo "Waiting for PostgreSQL..."
until node -e "const net=require('net'); const socket=net.connect(5432,'postgres'); socket.on('connect',()=>process.exit(0)); socket.on('error',()=>process.exit(1)); setTimeout(()=>process.exit(1),1000);"; do
  sleep 1
done

echo "Running migrations..."
npm run db:migrate

echo "Loading enrichment data..."
npm run db:seed-enrichment

echo "Starting API on port ${PORT:-8010}..."
npm run dev
