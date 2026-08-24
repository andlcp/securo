#!/bin/bash
# Deploy completo a partir da máquina de desenvolvimento.
#
#   bash scripts/ship.sh
#
# Compila o frontend aqui, carimba o dist com o commit atual, envia para
# o servidor e dispara o deploy. Compilar localmente em vez de no
# servidor é o que tira o passo mais caro do caminho: a máquina de
# produção tem 2 vCPUs dividas com outros projetos, e o build lá chegava
# a passar de 10 minutos.
#
# O carimbo é a rede de proteção: o deploy.sh compara com o HEAD do
# servidor e, se não bater, ignora o dist e compila por lá. Então esquecer
# de rodar este script nunca serve bundle velho — só volta a ser lento.
set -e

HOST="${SECURO_HOST:-andlcp@46.225.24.167}"
REMOTE_DIR="${SECURO_DIR:-/home/andlcp/securo}"

SHA=$(git rev-parse HEAD)

# Só arquivos rastreados importam: o vite compila a partir de src/, e
# rascunhos soltos na raiz não entram no bundle nem no commit.
if [ -n "$(git status --porcelain --untracked-files=no)" ]; then
  echo "Há alterações não commitadas em arquivos rastreados. O dist seria"
  echo "carimbado com ${SHA:0:7}, que não as inclui. Faça commit e push antes."
  exit 1
fi

echo "==> Compilando frontend (${SHA:0:7})..."
(cd frontend && npm run build)

echo "$SHA" > frontend/dist/.build-sha

echo "==> Enviando dist/ para $HOST..."
ssh "$HOST" "mkdir -p $REMOTE_DIR/frontend/dist"
# --delete para que arquivos de builds anteriores não fiquem para trás;
# os nomes têm hash, então sobras acumulariam sem nunca serem servidas.
if command -v rsync >/dev/null 2>&1; then
  rsync -az --delete frontend/dist/ "$HOST:$REMOTE_DIR/frontend/dist/"
else
  ssh "$HOST" "rm -rf $REMOTE_DIR/frontend/dist"
  scp -qr frontend/dist "$HOST:$REMOTE_DIR/frontend/"
fi

echo "==> Deploy no servidor..."
ssh "$HOST" "cd $REMOTE_DIR && bash deploy.sh"
