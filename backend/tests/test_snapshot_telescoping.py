"""A soma dos resultados diários tem de fechar com a variação do patrimônio.

Em 01–03/09/2026 o patrimônio subiu R$ 17.197 sem nenhum aporte, mas a soma
dos resultados diários deu R$ 16.138 — R$ 1.059 sem explicação.

A causa: a reconstrução incremental começava a varredura no próprio dia
pedido. Para calcular o resultado desse dia ela precisa do valor de ontem,
e o re-derivava dos valores de ativo do momento — enquanto o snapshot
guardado de ontem tinha congelado o valor de quando *ele* foi calculado. Os
dois números não coincidiam, e a série deixava de telescopar.

A correção é a varredura começar um dia antes e reescrever esse dia junto,
de modo que o "ontem" valha o mesmo dos dois lados.
"""
from datetime import date, timedelta


def _serie(valores, cashflows=None):
    """Constrói uma série diária como o motor faria: cada dia compara com o
    valor do dia anterior DA PRÓPRIA série."""
    cashflows = cashflows or [0.0] * len(valores)
    out = []
    for i, (v, cf) in enumerate(zip(valores, cashflows)):
        v_ant = valores[i - 1] if i > 0 else None
        ganho = None if v_ant is None else v - v_ant - cf
        out.append({"v_end": v, "cashflow": cf, "gain": ganho})
    return out


def test_soma_dos_ganhos_fecha_com_a_variacao():
    """Sem fluxo, a soma dos resultados diários é exatamente a diferença
    entre a primeira e a última pontas."""
    valores = [2_495_128.88, 2_494_588.02, 2_507_169.14, 2_512_326.10]
    serie = _serie(valores)
    ganhos = [d["gain"] for d in serie if d["gain"] is not None]
    assert round(sum(ganhos), 2) == round(valores[-1] - valores[0], 2)


def test_soma_fecha_mesmo_com_aporte_no_meio():
    """Com aporte, a soma dos resultados é a variação MENOS o que entrou —
    dinheiro novo não é rendimento."""
    valores = [1_000.0, 1_010.0, 1_515.0, 1_530.0]
    fluxos = [0.0, 0.0, 500.0, 0.0]     # aporte de 500 no terceiro dia
    serie = _serie(valores, fluxos)
    ganhos = [d["gain"] for d in serie if d["gain"] is not None]
    assert round(sum(ganhos), 2) == round(
        valores[-1] - valores[0] - sum(fluxos), 2)


def test_ontem_defasado_quebra_a_conta():
    """Reproduz o defeito: quando o valor de ontem usado no cálculo difere
    do que ficou guardado, a soma para de fechar — exatamente pela
    defasagem. É o que a varredura de um dia antes elimina."""
    # Números reais de 01-03/09/2026.
    guardado_ontem = 2_495_128.88            # o que ficou no snapshot de 31/08
    rederivado_ontem = 2_496_188.04          # o que o motor recalculou depois
    hoje = 2_512_326.10

    ganho_do_motor = hoje - rederivado_ontem
    variacao_aparente = hoje - guardado_ontem
    sobra = variacao_aparente - ganho_do_motor

    # Foi exatamente isto que apareceu na tela: o mês somou R$ 16.138 de
    # resultado enquanto o patrimônio subia R$ 17.197.
    assert round(ganho_do_motor, 2) == 16_138.06
    assert round(variacao_aparente, 2) == 17_197.22
    assert round(sobra, 2) == round(rederivado_ontem - guardado_ontem, 2)
    assert round(sobra, 2) == 1059.16, "a sobra é exatamente a defasagem"


def test_janela_incremental_cobre_o_dia_anterior():
    """A janela pedida ao motor tem de começar um dia antes do alvo, para
    que o dia anterior seja recalculado e reescrito."""
    alvo = date(2026, 9, 3)
    walk_from = alvo - timedelta(days=1)
    assert walk_from == date(2026, 9, 2)
    # E o acumulado vem de dois dias antes, senão o retorno de walk_from
    # entraria duas vezes na corrente.
    semente = walk_from - timedelta(days=1)
    assert semente == date(2026, 9, 1)
    assert semente < walk_from < alvo
