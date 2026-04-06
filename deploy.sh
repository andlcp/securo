#!/bin/bash
set -e

echo "==> Atualizando código..."
git pull origin main

echo "==> Compilando imagens..."
docker compose -f docker-compose.server.yml build

echo "==> Reiniciando serviços..."
docker compose -f docker-compose.server.yml up -d

echo "==> Limpando imagens antigas..."
docker image prune -f

echo ""
echo "Deploy concluído! Sistema rodando em http://46.225.24.167"
