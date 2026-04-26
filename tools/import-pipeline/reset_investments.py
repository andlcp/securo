"""
reset_investments.py
Apaga todos os ativos do tipo 'investment' do usuário autenticado no Securo.
Executa diretamente no banco via psql ou via API REST.

Uso (via API — recomendado):
    python reset_investments.py --url http://localhost:8000 --email seu@email.com --senha suasenha

Uso (modo dry-run para ver o que seria apagado):
    python reset_investments.py --url http://localhost:8000 --email seu@email.com --senha suasenha --dry-run
"""

import argparse
import sys

try:
    import requests
except ImportError:
    print("Dependência ausente. Instale com:")
    print("    pip install requests")
    sys.exit(1)


def main():
    parser = argparse.ArgumentParser(description="Apaga todos os investimentos do usuário")
    parser.add_argument('--url', default='http://localhost:8000', help='URL base da API Securo')
    parser.add_argument('--email', required=True)
    parser.add_argument('--senha', required=True)
    parser.add_argument('--dry-run', action='store_true', help='Apenas lista, não apaga')
    args = parser.parse_args()

    base = args.url.rstrip('/')

    # 1. Login
    print(f"Autenticando em {base}...")
    r = requests.post(f"{base}/api/auth/login",
                      data={'username': args.email, 'password': args.senha},
                      headers={'Content-Type': 'application/x-www-form-urlencoded'})
    if r.status_code != 200:
        print(f"Falha no login: {r.status_code} {r.text}")
        sys.exit(1)

    token = r.json().get('access_token')
    headers = {'Authorization': f'Bearer {token}'}
    print("✓ Autenticado\n")

    # 2. Listar ativos
    r = requests.get(f"{base}/api/assets", headers=headers)
    if r.status_code != 200:
        print(f"Falha ao listar ativos: {r.status_code} {r.text}")
        sys.exit(1)

    assets = r.json()
    investments = [a for a in assets if a.get('type') == 'investment']

    print(f"Total de ativos: {len(assets)}")
    print(f"Investimentos encontrados: {len(investments)}\n")

    if not investments:
        print("Nenhum investimento para apagar.")
        return

    print(f"{'ID':<38} {'NOME':<40} {'VALOR':>12}")
    print(f"{'─'*38} {'─'*40} {'─'*12}")
    for a in investments:
        nome = (a.get('name') or '')[:40]
        # valor aproximado: purchase_price ou último valor
        valor = a.get('purchase_price') or 0
        print(f"{str(a['id']):<38} {nome:<40} R$ {float(valor):>10,.2f}")

    if args.dry_run:
        print(f"\n[DRY RUN] {len(investments)} investimento(s) seriam apagados. Use sem --dry-run para confirmar.")
        return

    print(f"\nApagando {len(investments)} investimento(s)...")
    erros = []
    for i, a in enumerate(investments, 1):
        r = requests.delete(f"{base}/api/assets/{a['id']}", headers=headers)
        if r.status_code in (200, 204):
            print(f"  ✓ [{i}/{len(investments)}] {a.get('name', a['id'])}")
        else:
            print(f"  ✗ [{i}/{len(investments)}] {a.get('name', a['id'])} → {r.status_code}")
            erros.append(a['id'])

    print(f"\n{'─'*50}")
    print(f"✓ {len(investments) - len(erros)} apagados")
    if erros:
        print(f"✗ {len(erros)} com erro: {erros}")


if __name__ == '__main__':
    main()
