#!/bin/bash
set -e

echo "==> Atualizando código..."
git pull origin main

# O build do frontend (tsc + vite sobre ~3.500 módulos) é o passo mais
# caro do deploy: esta máquina tem 2 vCPUs dividas com outros projetos e
# o load chegava a 6+ durante a compilação. Como o ambiente de
# desenvolvimento já compila antes de cada push para checar tipos,
# refazer isso aqui é trabalho duplicado.
#
# Quando frontend/dist/ é enviado junto com um carimbo .build-sha que
# bate com o commit atual, usamos o Dockerfile.prebuilt — que só copia os
# arquivos para o nginx. Qualquer divergência (dist ausente, de outro
# commit) cai no build normal, então nunca servimos um bundle velho por
# esquecimento.
HEAD_SHA=$(git rev-parse HEAD)
STAMP=frontend/dist/.build-sha

if [ -f "$STAMP" ] && [ "$(cat "$STAMP")" = "$HEAD_SHA" ]; then
  export FRONTEND_DOCKERFILE=Dockerfile.prebuilt
  echo "==> Frontend: usando dist/ pré-compilado (${HEAD_SHA:0:7})"
elif [ -f "$STAMP" ]; then
  export FRONTEND_DOCKERFILE=Dockerfile
  echo "==> Frontend: dist/ é de $(cut -c1-7 "$STAMP"), esperado ${HEAD_SHA:0:7} — compilando aqui"
else
  export FRONTEND_DOCKERFILE=Dockerfile
  echo "==> Frontend: sem dist/ enviado — compilando aqui"
fi

echo "==> Compilando imagens..."
docker compose -f docker-compose.server.yml build

echo "==> Reiniciando serviços..."
docker compose -f docker-compose.server.yml up -d

echo "==> Limpando imagens antigas..."
docker image prune -f

echo ""
echo "Deploy concluído! Sistema rodando em http://46.225.24.167"
