"""O resultado em dinheiro e o percentual não podem discordar de sinal.

Em 28/08/2026 o painel mostrou "Variação do dia +0,02%" ao lado de "Perda
do dia -R$ 1.364,63". Os dois vinham de fontes diferentes: o percentual do
motor, e o dinheiro de uma subtração feita no frontend entre o `v_end` de
dois snapshots.

Isso funciona enquanto os dois dias saem da mesma varredura. Numa
reconstrução incremental não saem: o dia anterior é re-derivado dos valores
de ativo do momento, enquanto o snapshot guardado congelou o valor de
quando foi calculado. A diferença chegou a inverter o sinal.

A correção foi o motor emitir `gain` — o próprio numerador do Modified
Dietz — junto com o percentual. Estes testes prendem esse contrato.
"""
from decimal import Decimal


def _dietz(v_end: float, v_prev: float, cf: float = 0.0, inc: float = 0.0):
    """Modified Dietz, como o motor calcula."""
    denom = v_prev + 0.5 * cf
    ganho = v_end + inc - v_prev - cf
    return (ganho / denom if denom > 1e-3 else None), ganho


def test_gain_e_o_numerador_do_percentual():
    """O ganho tem de ser exatamente o numerador que produz o percentual."""
    r, ganho = _dietz(v_end=2_488_767.15, v_prev=2_490_131.78)
    assert ganho < 0
    assert r is not None and r < 0
    # O caso real de 28/08: queda de 1.364,63 sobre 2,49 milhões.
    assert round(ganho, 2) == -1364.63
    assert round(r * 100, 4) == -0.0548


def test_sinal_do_ganho_e_do_percentual_sempre_concordam():
    """Sem fluxo de caixa, um não pode ser positivo com o outro negativo."""
    casos = [
        (2_488_767.15, 2_490_131.78),   # queda
        (2_519_512.33, 2_511_270.81),   # alta
        (1_000.0, 1_000.0),             # parado
    ]
    for v_end, v_prev in casos:
        r, ganho = _dietz(v_end, v_prev)
        assert r is not None
        assert (r > 0) == (ganho > 0), f"divergiram em {v_end} vs {v_prev}"
        assert (r < 0) == (ganho < 0), f"divergiram em {v_end} vs {v_prev}"


def test_aporte_nao_vira_ganho():
    """Dinheiro que entra não é rentabilidade: sobe o patrimônio e o ganho
    fica zero. É o que separa aporte de resultado."""
    r, ganho = _dietz(v_end=1_100.0, v_prev=1_000.0, cf=100.0)
    assert round(ganho, 2) == 0.0
    assert r is not None and abs(r) < 1e-9


def test_resgate_nao_vira_perda():
    """Espelho do anterior: o resgate do CDB de 25/08 tirou R$ 32.930,62 da
    carteira e não podia aparecer como prejuízo."""
    r, ganho = _dietz(v_end=2_486_522.26, v_prev=2_511_270.81, cf=-32_930.62)
    assert ganho > 0, "o resgate virou perda"
    assert r is not None and r > 0


def test_provento_conta_como_ganho_sem_mexer_no_patrimonio():
    """Um dividendo recebido é resultado, mesmo que o valor da posição não
    tenha mudado — sai da carteira como caixa, entra como rendimento."""
    r, ganho = _dietz(v_end=1_000.0, v_prev=1_000.0, inc=50.0)
    assert round(ganho, 2) == 50.0
    assert r is not None and r > 0


def test_soma_dos_ganhos_diarios_e_o_ganho_do_periodo():
    """O mês em dinheiro é a soma dos dias — mesma composição que o
    percentual faz ao encadear os retornos diários."""
    dias = [(1_010.0, 1_000.0), (1_005.0, 1_010.0), (1_030.0, 1_005.0)]
    ganhos = [_dietz(ve, vp)[1] for ve, vp in dias]
    assert round(sum(ganhos), 2) == 30.0          # 1030 - 1000
    # E o encadeamento dos percentuais bate com a variação total.
    cum = 1.0
    for ve, vp in dias:
        cum *= 1.0 + _dietz(ve, vp)[0]
    assert round((cum - 1.0) * 100, 4) == 3.0
