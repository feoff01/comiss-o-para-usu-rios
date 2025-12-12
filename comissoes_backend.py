# comissoes_backend.py

import pandas as pd
import numpy as np
from datetime import datetime
from pandas.tseries.offsets import MonthEnd
import locale

# Tenta configurar locale pt-BR, mas não quebra se não conseguir
for loc in ['pt_BR.UTF-8', 'pt_BR.utf8', 'pt_BR', 'Portuguese_Brazil.1252']:
    try:
        locale.setlocale(locale.LC_TIME, loc)
        break
    except locale.Error:
        continue


def calcular_comissoes(pj1, seg, cam, co_ter, co_xpvp, cre, xpcs, lan_man, tim_rep, lan_pro):
    """
    Recebe todos os DataFrames já lidos (pj1, seg, cam, co_ter, co_xpvp, cre, xpcs,
    lan_man, tim_rep, lan_pro) e devolve:

      - df_final: resumo por assessor (inclui 'Valor Total Assessor')
      - df_juntar: base detalhada por assessor/cliente/produto/percentual

    A lógica é baseada na sua versão mais recente do script.
    """

    # ======================
    # 0) Cópias de segurança
    # ======================
    pj1 = pj1.copy()
    seg = seg.copy()
    cam = cam.copy()
    co_ter = co_ter.copy()
    co_xpvp = co_xpvp.copy()
    cre = cre.copy()
    xpcs = xpcs.copy()
    lan_man = lan_man.copy()
    tim_rep = tim_rep.copy()
    lan_pro = lan_pro.copy()

    # ==========================================
    # 1) Tratamento para os repasses dos produtos
    # ==========================================
    tim_rep.rename({
        '% RV': 'Repasse Investimento RV',
        '% RF': 'Repasse Investimento RF',
        '% Outros Investimentos': 'Repasse Investimento Outros',
        '% PJ2': 'Repasse Investimento PJ2',
        '% Líder': 'Repasse Investimento Líder',
        '% Mesa RV': 'Repasse Investimento Mesa RV',
        '% Mesa RF': 'Repasse Investimento Mesa RF',
        '% Co-Corretagem Assessor': 'Repasse Investimento Co-Corretagem Assessor',
        '% Co-Corretagem Capitão': 'Repasse Investimento Co-Corretagem Capitão',
        '% Mesa Trader': 'Repasse Investimento Mesa Trader',
        '% Trader Assessor': 'Repasse Investimento Trader Assessor'
    }, axis=1, inplace=True)

    tim_rep["Tipo Repasse 1"] = "Investimentos - RV"
    tim_rep["Tipo Repasse 2"] = "Investimentos - RF"
    tim_rep["Tipo Repasse 3"] = "Investimentos - Outros"
    tim_rep["Tipo Repasse 4"] = "PJ2"
    tim_rep["Tipo Repasse 5"] = "Líder"
    tim_rep["Tipo Repasse 6"] = "Mesa RV"
    tim_rep["Tipo Repasse 7"] = "Mesa RF"
    tim_rep["Tipo Repasse 8"] = "Co-corretagem - Assessor"
    tim_rep["Tipo Repasse 9"] = "Co-corretagem - Capitão"
    tim_rep["Tipo Repasse 10"] = "Mesa Trader"
    tim_rep["Tipo Repasse 11"] = "Trader Assessor"

    mapa_tipos = {
        'Repasse Investimento RV': 'Investimentos - RV',
        'Repasse Investimento RF': 'Investimentos - RF',
        'Repasse Investimento Outros': 'Investimentos - Outros',
        'Repasse Investimento PJ2': 'PJ2',
        'Repasse Investimento Líder': 'Líder',
        'Repasse Investimento Mesa RV': 'Mesa RV',
        'Repasse Investimento Mesa RF': 'Mesa RF',
        'Repasse Investimento Co-Corretagem Assessor': 'Co-corretagem - Assessor',
        'Repasse Investimento Co-Corretagem Capitão': 'Co-corretagem - Capitão',
        'Repasse Investimento Mesa Trader': 'Mesa Trader',
        'Repasse Investimento Trader Assessor': 'Trader Assessor'
    }

    colunas_fixas = [
        'Código', 'Nome Completo', 'Líder',
        'Posição', 'Imposto + Despesa', 'Comisssionado'
    ]

    repasse_linhas = tim_rep.melt(
        id_vars=colunas_fixas,
        value_vars=list(mapa_tipos.keys()),
        var_name='Tipo Repasse Original',
        value_name='% Repasse'
    )

    repasse_linhas['Tipo Repasse'] = repasse_linhas['Tipo Repasse Original'].map(mapa_tipos)

    # =========================
    # 2) Tratamento da base PJ1
    # =========================
    pj1["Valor Assessor"] = (
        pj1["Comissão (R$) Assessor Direto"]
        + pj1["Comissão (R$) Assessor Indireto I"]
        + pj1["Comissão (R$) Assessor Indireto II"]
        + pj1["Comissão (R$) Assessor Indireto III"]
    )
    pj1["PJ"] = "PJ1"
    pj1["ID"] = range(1, len(pj1) + 1)
    pj1["Data"] = pd.to_datetime(pj1["Data"], dayfirst=True, errors='coerce')
    pj1["Data Fechamento"] = pj1["Data"] + MonthEnd(0)
    pj1["Data"] = pj1["Data"].dt.strftime("%d/%m/%Y")
    pj1["Data Fechamento"] = pj1["Data Fechamento"].dt.strftime("%d/%m/%Y")

    # ======================
    # 3) Totais por produto
    # ======================

    # ----- SEGURO -----
    seg_final = seg.merge(
        tim_rep[[
            'Código', 'Imposto + Despesa',
            'Repasse Investimento Co-Corretagem Assessor',
            'Repasse Investimento Líder',
            "Repasse Investimento Co-Corretagem Capitão"
        ]],
        left_on='Código Assessor',
        right_on='Código',
        how='left'
    )
    seg_final["Valor Imposto"] = seg_final["Comissão Escritório"] * seg_final["Imposto + Despesa"]
    seg_final["Sem Imposto"] = (
        seg_final["Comissão Escritório"]
        - (seg_final["Comissão Escritório"] * seg_final["Imposto + Despesa"])
    )
    seg_final["Valor Assessor Seguro"] = (
        seg_final["Sem Imposto"] * seg_final["Repasse Investimento Co-Corretagem Assessor"]
    )
    seg_final["Valor Capitão Seguro"] = (
        seg_final["Sem Imposto"] * seg_final["Repasse Investimento Co-Corretagem Capitão"]
    )
    seg_final[["Valor Assessor Seguro", "Valor Capitão Seguro"]] = seg_final[
        ["Valor Assessor Seguro", "Valor Capitão Seguro"]
    ].fillna(0)

    # ----- CÂMBIO -----
    cam_final = cam.merge(
        tim_rep[[
            'Código', 'Imposto + Despesa',
            'Repasse Investimento PJ2',
            'Repasse Investimento Líder'
        ]],
        left_on='Código Assessor',
        right_on='Código',
        how='left'
    )
    cam_final["Valor Imposto"] = cam_final["Comissão Escritório"] * cam_final["Imposto + Despesa"]
    cam_final["Sem Imposto"] = (
        cam_final["Comissão Escritório"]
        - (cam_final["Comissão Escritório"] * cam_final["Imposto + Despesa"])
    )
    cam_final["Valor Assessor Câmbio"] = cam_final["Sem Imposto"] * cam_final["Repasse Investimento PJ2"]
    cam_final[["Valor Assessor Câmbio"]] = cam_final[["Valor Assessor Câmbio"]].fillna(0)

    # ----- CO-CORRETAGEM TERCEIRAS -----
    co_ter_final = co_ter.merge(
        tim_rep[[
            'Código', 'Imposto + Despesa',
            'Repasse Investimento PJ2',
            'Repasse Investimento Líder',
            "Repasse Investimento Co-Corretagem Assessor",
            "Repasse Investimento Co-Corretagem Capitão"
        ]],
        left_on='Código Assessor',
        right_on='Código',
        how='left'
    )
    co_ter_final["Valor Imposto"] = (
        co_ter_final["Comissão Escritório"] * co_ter_final["Imposto + Despesa"]
    )
    co_ter_final["Sem Imposto"] = (
        co_ter_final["Comissão Escritório"]
        - (co_ter_final["Comissão Escritório"] * co_ter_final["Imposto + Despesa"])
    )
    co_ter_final["Valor Assessor Co-Corretagem Terceiras"] = (
        co_ter_final["Sem Imposto"] * co_ter_final["Repasse Investimento Co-Corretagem Assessor"]
    )
    co_ter_final["Valor Capitão Co-Corretagem Terceiras"] = (
        co_ter_final["Sem Imposto"] * co_ter_final["Repasse Investimento Co-Corretagem Capitão"]
    )
    co_ter_final[[
        "Valor Assessor Co-Corretagem Terceiras",
        "Valor Capitão Co-Corretagem Terceiras"
    ]] = co_ter_final[[
        "Valor Assessor Co-Corretagem Terceiras",
        "Valor Capitão Co-Corretagem Terceiras"
    ]].fillna(0)

    # ----- CO-CORRETAGEM XPVP -----
    co_xpvp_final = co_xpvp.merge(
        tim_rep[[
            'Código', 'Imposto + Despesa',
            'Repasse Investimento PJ2',
            'Repasse Investimento Líder',
            "Repasse Investimento Co-Corretagem Assessor",
            "Repasse Investimento Co-Corretagem Capitão"
        ]],
        left_on='Código Assessor',
        right_on='Código',
        how='left'
    )
    co_xpvp_final["Valor Imposto"] = (
        co_xpvp_final["Comissão Escritório"] * co_xpvp_final["Imposto + Despesa"]
    )
    co_xpvp_final["Sem Imposto"] = (
        co_xpvp_final["Comissão Escritório"]
        - (co_xpvp_final["Comissão Escritório"] * co_xpvp_final["Imposto + Despesa"])
    )
    co_xpvp_final["Valor Assessor Co-Corretagem XPVP"] = (
        co_xpvp_final["Sem Imposto"] * co_xpvp_final["Repasse Investimento PJ2"]
    )

    # ----- CRÉDITO -----
    cre_final = cre.merge(
        tim_rep[[
            'Código', 'Imposto + Despesa',
            'Repasse Investimento PJ2',
            'Repasse Investimento Líder'
        ]],
        left_on='Código Assessor',
        right_on='Código',
        how='left'
    )
    cre_final["Valor Imposto"] = cre_final["Comissão Escritório"] * cre_final["Imposto + Despesa"]
    cre_final["Sem Imposto"] = (
        cre_final["Comissão Escritório"]
        - (cre_final["Comissão Escritório"] * cre_final["Imposto + Despesa"])
    )
    cre_final["Valor Assessor Crédito"] = cre_final["Sem Imposto"] * cre_final["Repasse Investimento PJ2"]
    cre_final[["Valor Assessor Crédito"]] = cre_final[["Valor Assessor Crédito"]].fillna(0)

    # ----- XPCS -----
    xpcs_final = xpcs.merge(
        tim_rep[[
            'Código', 'Imposto + Despesa',
            'Repasse Investimento PJ2',
            'Repasse Investimento Líder'
        ]],
        left_on='Código Assessor',
        right_on='Código',
        how='left'
    )
    xpcs_final["Valor Imposto"] = xpcs_final["Comissão Escritório"] * xpcs_final["Imposto + Despesa"]
    xpcs_final["Sem Imposto"] = (
        xpcs_final["Comissão Escritório"]
        - (xpcs_final["Comissão Escritório"] * xpcs_final["Imposto + Despesa"])
    )
    xpcs_final["Valor Assessor XPCS"] = xpcs_final["Sem Imposto"] * xpcs_final["Repasse Investimento PJ2"]
    xpcs_final[["Valor Assessor XPCS"]] = xpcs_final[["Valor Assessor XPCS"]].fillna(0)

    # ========================================================
    # 4) Desconto de Transferência de Clientes (tratamento PJ1)
    # ========================================================
    pj1_desc = pj1[[
        "ID", "PJ", "Categoria", "Produto",
        "Cód. Assessor Direto", "Comissão Bruta (R$) Escritório"
    ]].copy()
    pj1_desc = pj1_desc[pj1_desc["Produto"] == "Desconto de Transferência de Clientes"]

    pj1_desc_2 = pj1_desc.groupby(
        ["PJ", "Cód. Assessor Direto", "Categoria", "Produto"]
    )[["Comissão Bruta (R$) Escritório"]].sum().reset_index()

    pj1_desc_2.rename(
        {'Comissão Bruta (R$) Escritório': 'Comissão Escritório Soma'},
        axis=1,
        inplace=True
    )

    # Valores positivos (viram produto separado)
    pj1_desc_pos = pj1_desc_2[pj1_desc_2['Comissão Escritório Soma'] > 0].copy()
    pj1_desc_pos["Comissão Escritório Tratada"] = pj1_desc_pos["Comissão Escritório Soma"]
    pj1_desc_pos["Produto"] = "Desconto de Transferência de Clientes Positivo"

    # Base pj1 sem campanhas / desconto bruto
    pj1_final = pj1[~pj1['Produto'].isin([
        'Campanha COE', 'Campanha Renda Variável',
        'Campanhas', 'Desconto de Transferência de Clientes'
    ])]

    pj1_perc = pj1_final[[
        "ID", "PJ", "Cód. Assessor Direto",
        "Categoria", "Produto", "Comissão Bruta (R$) Escritório"
    ]].copy()

    # Retirando os códigos com desconto positivo
    pj1_perc = pj1_perc[
        ~pj1_perc["Cód. Assessor Direto"].isin(pj1_desc_pos["Cód. Assessor Direto"])
    ].copy()

    pj1_perc["Comissão Escritório Soma x Produto"] = (
        pj1_perc.groupby(["Cód. Assessor Direto", "Categoria", "Produto"])["Comissão Bruta (R$) Escritório"]
        .transform("sum")
    )

    pj1_perc["Proporção Desconto Transferência"] = (
        pj1_perc["Comissão Bruta (R$) Escritório"]
        / pj1_perc["Comissão Escritório Soma x Produto"]
        * 100
    )

    # Produto com maior soma por assessor
    maior_produto = (
        pj1_perc.groupby(["Cód. Assessor Direto", "Produto"])["Comissão Escritório Soma x Produto"]
        .first()
        .reset_index()
    )

    maior_produto = maior_produto.loc[
        maior_produto.groupby("Cód. Assessor Direto")["Comissão Escritório Soma x Produto"].idxmax()
    ]

    pj1_maior = pj1_perc.merge(
        maior_produto[["Cód. Assessor Direto", "Produto"]],
        on=["Cód. Assessor Direto", "Produto"],
        how="inner"
    ).reset_index(drop=True)

    pj1_desc3 = pj1_maior[["Cód. Assessor Direto", "Produto"]].drop_duplicates()
    pj1_desc3.rename({'Produto': 'Descontar de'}, axis=1, inplace=True)

    pj1_desc_2 = pj1_desc_2.merge(pj1_desc3, on="Cód. Assessor Direto", how="left")

    pj1_desc_4 = pj1_maior.merge(
        pj1_desc_2[["Cód. Assessor Direto", "Comissão Escritório Soma"]],
        on="Cód. Assessor Direto",
        how="left"
    )
    pj1_desc_4["Desconto de Transferência de Clientes Fracionado"] = (
        pj1_desc_4["Proporção Desconto Transferência"]
        * pj1_desc_4["Comissão Escritório Soma"] / 100
    )
    pj1_desc_4["Comissão Escritório Tratada"] = (
        pj1_desc_4["Comissão Bruta (R$) Escritório"]
        + pj1_desc_4["Desconto de Transferência de Clientes Fracionado"]
    )
    pj1_desc_4 = pj1_desc_4[pj1_desc_4["Comissão Escritório Tratada"].notnull()]

    pj1_final = pj1.merge(
        pj1_desc_4,
        on=[
            "ID", "PJ", "Cód. Assessor Direto",
            "Categoria", "Produto",
            "Comissão Bruta (R$) Escritório"
        ],
        how="left"
    )

    # linhas com desconto positivo
    colunas_comuns = [
        "PJ", "Cód. Assessor Direto", "Categoria",
        "Produto", "Comissão Escritório Soma"
    ]
    if pj1_desc_pos.shape[1] == 1 and "Cód. Assessor Direto" in pj1_desc_pos.columns:
        pj1_desc_pos = pj1[
            pj1["Cód. Assessor Direto"].isin(pj1_desc_pos["Cód. Assessor Direto"])
        ][colunas_comuns].drop_duplicates()

    pj1_final = pd.concat([pj1_final, pj1_desc_pos], ignore_index=True)

    # ==========================================
    # 5) Tipo de repasse por categoria / produto
    # ==========================================
    mapa_categoria_repasse = {
        "Renda Variável": "Investimentos - RV",
        "Produtos Financeiros": "Investimentos - RV",
        "Fundos Imobiliários": "Investimentos - RV",
        "Renda Fixa": "Investimentos - RF"
    }

    pj1_final["Tipo Repasse Baseado na Categoria"] = pj1_final["Categoria"].map(mapa_categoria_repasse)
    pj1_final["Tipo Repasse Baseado na Categoria"] = pj1_final[
        "Tipo Repasse Baseado na Categoria"
    ].fillna("Investimentos - Outros")

    pj1_final.loc[
        pj1_final["Produto"].isin(["BM&F Ontick", "BM&F Ontick Parceiros"]),
        "Tipo Repasse Baseado na Categoria"
    ] = "Trader Assessor"
    pj1_final.loc[pj1_final["Produto"] == "COE", "Tipo Repasse Baseado na Categoria"] = "Investimentos - Outros"
    pj1_final.loc[
        pj1_final["Produto"].isin(["BM&F Ontick", "BM&F Ontick Parceiros"]),
        "PJ"
    ] = "PJ2"

    pj1_final = pj1_final.merge(
        repasse_linhas[["Código", "Tipo Repasse", "% Repasse", "Imposto + Despesa"]],
        left_on=["Cód. Assessor Direto", "Tipo Repasse Baseado na Categoria"],
        right_on=["Código", "Tipo Repasse"],
        how="left"
    )
    pj1_final["percentual tratado"] = pj1_final["% Repasse"]
    pj1_final.drop(columns=["Código", "Tipo Repasse", "% Repasse"], inplace=True, errors="ignore")

    # Repasse Mesa RV/RF/Trader
    repasse_mesa_rv = repasse_linhas[
        repasse_linhas["Tipo Repasse"] == "Mesa RV"
    ][["Código", "% Repasse"]].rename(columns={"% Repasse": "% Repasse Mesa RV"})

    repasse_mesa_rf = repasse_linhas[
        repasse_linhas["Tipo Repasse"] == "Mesa RF"
    ][["Código", "% Repasse"]].rename(columns={"% Repasse": "% Repasse Mesa RF"})

    repasse_mesa_trade = repasse_linhas[
        repasse_linhas["Tipo Repasse"] == "Mesa Trader"
    ][["Código", "% Repasse"]].rename(columns={"% Repasse": "% Repasse Mesa Trader"})

    pj1_final = pj1_final.merge(
        repasse_mesa_rv,
        left_on="Cód. Assessor Direto",
        right_on="Código",
        how="left"
    ).drop(columns=["Código"], errors="ignore")

    pj1_final = pj1_final.merge(
        repasse_mesa_rf,
        left_on="Cód. Assessor Direto",
        right_on="Código",
        how="left"
    ).drop(columns=["Código"], errors="ignore")

    pj1_final = pj1_final.merge(
        repasse_mesa_trade,
        left_on="Cód. Assessor Direto",
        right_on="Código",
        how="left"
    ).drop(columns=["Código"], errors="ignore")

    pj1_final["percentual tratado mesa rv"] = np.where(
        pj1_final["Tipo Repasse Baseado na Categoria"] == "Investimentos - RV",
        pj1_final["% Repasse Mesa RV"],
        0
    )
    pj1_final["percentual tratado mesa rf"] = np.where(
        pj1_final["Tipo Repasse Baseado na Categoria"] == "Investimentos - RF",
        pj1_final["% Repasse Mesa RF"],
        0
    )
    pj1_final["percentual tratado mesa trader"] = np.where(
        pj1_final["Tipo Repasse Baseado na Categoria"] == "Trader Assessor",
        pj1_final["% Repasse Mesa Trader"],
        0
    )

    # =====================================
    # 6) Cálculos finais de PJ1
    # =====================================
    pj1_final["Comissão Escritório Tratada"] = pj1_final["Comissão Escritório Tratada"].fillna(
        pj1["Comissão Bruta (R$) Escritório"]
    )
    pj1_final["Sem Imposto"] = (
        pj1_final["Comissão Escritório Tratada"]
        - (pj1_final["Comissão Escritório Tratada"] * pj1_final["Imposto + Despesa"])
    )
    pj1_final["Valor Imposto"] = (
        pj1_final["Comissão Escritório Tratada"] * pj1_final["Imposto + Despesa"]
    )
    pj1_final["Valor Assessor Direto"] = pj1_final["Sem Imposto"] * pj1_final["percentual tratado"]
    pj1_final[["Valor Assessor Direto"]] = pj1_final[["Valor Assessor Direto"]].fillna(0)

    pj1_final["Valor Mesa RV"] = pj1_final["Sem Imposto"] * pj1_final["percentual tratado mesa rv"]
    pj1_final["Valor Mesa RF"] = pj1_final["Sem Imposto"] * pj1_final["percentual tratado mesa rf"]
    pj1_final["Valor Mesa Trader"] = pj1_final["Sem Imposto"] * pj1_final["percentual tratado mesa trader"]

    # Ajustes para Murilo (A39437) + produtos especiais
    produtos_somente_a39437 = ['BM&F', 'BM&F Mini', 'BM&F Self Service']
    produtos_todos = ['BOVESPA FIIs Empacotados', 'BOVESPA FIIs Risco']

    cond1 = (
        pj1_final['Produto'].isin(produtos_somente_a39437)
        & (pj1_final['Cód. Assessor Direto'] == 'A39437')
    )
    cond2 = pj1_final['Produto'].isin(produtos_todos)
    pj1_final.loc[cond1 | cond2, 'Valor Mesa RV'] = 0

    # =====================================
    # 7) Lançamento de Produtos (lan_pro)
    # =====================================
    mapa_categoria_repasse_lan_pro = {
        "seguro auto": "PJ2",
        "cripto": "Cripto",
        "consorcio": "Consorcio",
        "convenio": "Convenio"
    }

    lan_pro["Tipo Repasse Baseado na Categoria"] = lan_pro["Categoria"].map(
        mapa_categoria_repasse_lan_pro
    )

    lan_pro = lan_pro.merge(
        repasse_linhas[["Código", "Tipo Repasse", "% Repasse", "Imposto + Despesa"]],
        left_on=["Código do Assessor", "Tipo Repasse Baseado na Categoria"],
        right_on=["Código", "Tipo Repasse"],
        how="left"
    )
    lan_pro = lan_pro.drop(columns=["Código"], errors="ignore")
    lan_pro["percentual tratado"] = lan_pro["% Repasse"]

    imposto = lan_pro["Imposto + Despesa"].fillna(0)

    lan_pro["Sem Imposto"] = np.where(
        (imposto == 0),
        0,
        lan_pro["Comissão Escritório"] - (lan_pro["Comissão Escritório"] * imposto)
    )
    lan_pro["Valor Imposto"] = np.where(
        (imposto == 0),
        0,
        (lan_pro["Comissão Escritório"] * imposto)
    )
    lan_pro["Valor Lançamentos Produtos"] = np.where(
        (imposto == 0),
        lan_pro["Comissão Escritório"],
        (lan_pro["Sem Imposto"] * lan_pro["percentual tratado"])
    )

    lan_pro = lan_pro.merge(
        tim_rep[['Código', 'Repasse Investimento Líder']],
        left_on='Código do Assessor',
        right_on='Código',
        how='left'
    ).drop(columns=["Código"], errors="ignore")

    # =====================================
    # 8) Agrupamentos PJ1 e demais
    # =====================================
    pj1_final_group = pj1_final[~pj1_final['Produto'].isin([
        'Campanha COE', 'Campanha Renda Variável',
        'Campanhas', 'Desconto de Transferência de Clientes'
    ])]

    pj1_d = pj1_final_group.groupby(['Cód. Assessor Direto'])[
        ["Valor Assessor Direto"]
    ].sum().reset_index()
    pj1_i1 = pj1_final_group.groupby(['Cód. Assessor Indireto I'])[
        ["Comissão (R$) Assessor Indireto I"]
    ].sum().reset_index()
    pj1_i2 = pj1_final_group.groupby(['Cód. Assessor Indireto II'])[
        ["Comissão (R$) Assessor Indireto II"]
    ].sum().reset_index()
    pj1_i3 = pj1_final_group.groupby(['Cód. Assessor Indireto III'])[
        ["Comissão (R$) Assessor Indireto III"]
    ].sum().reset_index()

    df_merged = pd.merge(
        pj1_d, pj1_i1,
        left_on='Cód. Assessor Direto',
        right_on='Cód. Assessor Indireto I',
        how='outer'
    )
    df_merged = pd.merge(
        df_merged, pj1_i2,
        left_on='Cód. Assessor Direto',
        right_on='Cód. Assessor Indireto II',
        how='outer'
    )
    pj1_group = pd.merge(
        df_merged, pj1_i3,
        left_on='Cód. Assessor Direto',
        right_on='Cód. Assessor Indireto III',
        how='outer'
    )

    pj1_group[[
        'Valor Assessor Direto',
        'Comissão (R$) Assessor Indireto I',
        'Comissão (R$) Assessor Indireto II',
        'Comissão (R$) Assessor Indireto III'
    ]] = pj1_group[[
        'Valor Assessor Direto',
        'Comissão (R$) Assessor Indireto I',
        'Comissão (R$) Assessor Indireto II',
        'Comissão (R$) Assessor Indireto III'
    ]].fillna(0)

    pj1_group["Valor Assessor PJ1"] = (
        pj1_group["Valor Assessor Direto"]
        + pj1_group['Comissão (R$) Assessor Indireto I']
        + pj1_group['Comissão (R$) Assessor Indireto II']
        + pj1_group['Comissão (R$) Assessor Indireto III']
    )

    pj1_group = pj1_group[["Cód. Assessor Direto", "Valor Assessor PJ1"]]

    seg_group = seg_final.groupby(["Código Assessor"])[
        ["Valor Assessor Seguro", "Valor Capitão Seguro"]
    ].sum().reset_index()
    cam_group = cam_final.groupby(["Código Assessor"])[
        ["Valor Assessor Câmbio"]
    ].sum().reset_index()
    co_ter_group = co_ter_final.groupby(["Código Assessor"])[[
        "Valor Assessor Co-Corretagem Terceiras",
        "Valor Capitão Co-Corretagem Terceiras"
    ]].sum().reset_index()
    cre_group = cre_final.groupby(["Código Assessor"])[
        ["Valor Assessor Crédito"]
    ].sum().reset_index()
    xpcs_group = xpcs_final.groupby(["Código Assessor"])[
        ["Valor Assessor XPCS"]
    ].sum().reset_index()
    co_xpvp_group = co_xpvp_final.groupby(["Código Assessor"])[
        ["Valor Assessor Co-Corretagem XPVP"]
    ].sum().reset_index()

    # =====================================
    # 9) Lançamentos manuais e produtos
    # =====================================
    lan_man["Produto"] = lan_man["Produto"] + " - " + lan_man["Nome Completo"]
    lan_man_group = lan_man.groupby(["Código"])[["Valor"]].sum().reset_index()
    lan_man_group = lan_man_group.rename(
        columns={'Valor': 'Valor Lançamentos Manuais', 'Código': 'Código Assessor'}
    )

    lan_pro_filtrado = lan_pro[lan_pro["Categoria"] != "mesa"].copy()
    lan_pro_group = lan_pro_filtrado.groupby(["Código do Assessor"])[
        ["Valor Lançamentos Produtos"]
    ].sum().reset_index()
    lan_pro_group = lan_pro_group.rename(columns={'Código do Assessor': 'Código Assessor'})

    # Ajustes A97601 e A50753
    linhas_negativas = lan_man[lan_man["Código"] == "A50753"].copy()
    linhas_negativas["Valor"] *= -1
    lan_man = pd.concat([lan_man, linhas_negativas], ignore_index=True)
    lan_man["Valor negativado"] = lan_man["Valor"] * -1

    soma_A97601 = lan_man[lan_man['Código'] == 'A97601']['Valor'].sum()
    lan_man_A97601 = lan_man[lan_man["Debitar de"] == "A97601"]
    lan_man_A97601 = lan_man_A97601["Valor negativado"].sum()
    valor_total_97601 = lan_man_A97601 + soma_A97601

    if "A97601" in lan_man_group["Código Assessor"].values:
        lan_man_group.loc[
            lan_man_group["Código Assessor"] == "A97601",
            "Valor Lançamentos Manuais"
        ] = valor_total_97601
    else:
        nova_linha = pd.DataFrame({
            "Código Assessor": ["A97601"],
            "Valor Lançamentos Manuais": [valor_total_97601]
        })
        lan_man_group = pd.concat([lan_man_group, nova_linha], ignore_index=True)

    lan_man_A50753 = lan_man[lan_man["Debitar de"] == "A50753"]
    lan_man_A50753 = lan_man_A50753["Valor negativado"].sum()

    if "A50753" in lan_man_group["Código Assessor"].values:
        lan_man_group.loc[
            lan_man_group["Código Assessor"] == "A50753",
            "Valor Lançamentos Manuais"
        ] = lan_man_A50753
    else:
        nova_linha = pd.DataFrame({
            "Código Assessor": ["A50753"],
            "Valor Lançamentos Manuais": [lan_man_A50753]
        })
        lan_man_group = pd.concat([lan_man_group, nova_linha], ignore_index=True)

    # =====================================
    # 10) Ajustes de mesa (A21426, A54626, A39437)
    # =====================================
    soma_valor_direto_A21426 = pj1_final.loc[
        pj1_final['Cód. Assessor Direto'] == 'A21426',
        'Valor Assessor Direto'
    ].sum()
    soma_valor_mesa_rv = pj1_final['Valor Mesa RV'].sum()
    novo_valor_rv_A21426 = soma_valor_direto_A21426 + soma_valor_mesa_rv
    pj1_group.loc[
        pj1_group['Cód. Assessor Direto'] == 'A21426',
        'Valor Assessor PJ1'
    ] = novo_valor_rv_A21426

    soma_valor_direto_A54626 = pj1_final.loc[
        pj1_final['Cód. Assessor Direto'] == 'A54626',
        'Valor Assessor Direto'
    ].sum()
    soma_valor_mesa_rf = pj1_final['Valor Mesa RF'].sum()
    novo_valor_rv_A54626 = soma_valor_direto_A54626 + soma_valor_mesa_rf
    pj1_group.loc[
        pj1_group['Cód. Assessor Direto'] == 'A54626',
        'Valor Assessor PJ1'
    ] = novo_valor_rv_A54626

    soma_valor_direto_A39437 = pj1_final.loc[
        pj1_final['Cód. Assessor Direto'] == 'A39437',
        'Valor Assessor Direto'
    ].sum()
    soma_valor_mesa_trader = pj1_final['Valor Mesa Trader'].sum()
    novo_valor_trader_A39437 = soma_valor_direto_A39437 + soma_valor_mesa_trader
    pj1_group.loc[
        pj1_group['Cód. Assessor Direto'] == 'A39437',
        'Valor Assessor PJ1'
    ] = novo_valor_trader_A39437

    # =====================================
    # 11) Montagem df_final (resumo por assessor)
    # =====================================
    pj1_final = pj1_final.merge(
        tim_rep[['Código', 'Repasse Investimento Líder']],
        left_on='Cód. Assessor Direto',
        right_on='Código',
        how='left'
    )

    # Lista de todos os DataFrames que terminam com "_group"
    dataframes = [
        pj1_group, seg_group, cam_group,
        co_ter_group, co_xpvp_group,
        cre_group, xpcs_group,
        lan_man_group, lan_pro_group
    ]

    chaves = [
        'Cód. Assessor Direto',
        'Código Assessor',
        'Código Assessor',
        'Código Assessor',
        'Código Assessor',
        'Código Assessor',
        'Código Assessor',
        'Código Assessor',
        'Código Assessor'
    ]

    df_final = dataframes[0].rename(columns={chaves[0]: 'Código Assessor'})
    for i in range(1, len(dataframes)):
        df_temp = dataframes[i].rename(columns={chaves[i]: 'Código Assessor'})
        df_final = pd.merge(df_final, df_temp, on='Código Assessor', how='outer')

    df_final = df_final.fillna(0)

    # Capitão Co-corretagem (A70108)
    soma_valor_seguros = (
        df_final['Valor Capitão Seguro'].sum()
        + df_final["Valor Capitão Co-Corretagem Terceiras"].sum()
    )
    df_final["Total Capitão Co-Corretagem"] = (
        df_final["Valor Capitão Co-Corretagem Terceiras"]
        + df_final["Valor Capitão Seguro"]
    )
    df_final.loc[
        df_final['Código Assessor'] == 'A70108',
        "Total Capitão Co-Corretagem"
    ] = soma_valor_seguros

    # =====================================
    # 12) Valores de Líder (A66943)
    # =====================================
    seg_final["Valor Lider"] = seg_final["Sem Imposto"] * seg_final["Repasse Investimento Líder"]
    cam_final["Valor Lider"] = cam_final["Sem Imposto"] * cam_final["Repasse Investimento Líder"]
    co_ter_final["Valor Lider"] = co_ter_final["Sem Imposto"] * co_ter_final["Repasse Investimento Líder"]
    cre_final["Valor Lider"] = cre_final["Sem Imposto"] * cre_final["Repasse Investimento Líder"]
    xpcs_final["Valor Lider"] = xpcs_final["Sem Imposto"] * xpcs_final["Repasse Investimento Líder"]
    co_xpvp_final["Valor Lider"] = co_xpvp_final["Sem Imposto"] * co_xpvp_final["Repasse Investimento Líder"]

    lan_pro_filtrado = lan_pro_filtrado[lan_pro_filtrado['Produto'].notna()]
    lan_pro_filtrado["Valor Lançamentos Produtos"] = lan_pro_filtrado["Valor Lançamentos Produtos"].fillna(0)
    lan_pro_filtrado["Valor Lider"] = (
        lan_pro_filtrado["Valor Lançamentos Produtos"] * lan_pro_filtrado["Repasse Investimento Líder"]
    )

    seg_group_lider = seg_final.groupby(["Código Assessor"])[["Valor Lider"]].sum().reset_index()
    cam_group_lider = cam_final.groupby(["Código Assessor"])[["Valor Lider"]].sum().reset_index()
    co_ter_group_lider = co_ter_final.groupby(["Código Assessor"])[["Valor Lider"]].sum().reset_index()
    cre_group_lider = cre_final.groupby(["Código Assessor"])[["Valor Lider"]].sum().reset_index()
    xpcs_group_lider = xpcs_final.groupby(["Código Assessor"])[["Valor Lider"]].sum().reset_index()
    co_xpvp_group_lider = co_xpvp_final.groupby(["Código Assessor"])[["Valor Lider"]].sum().reset_index()
    lan_pro_group_lider = lan_pro_filtrado.groupby(["Código do Assessor"])[["Valor Lider"]].sum().reset_index()
    lan_pro_group_lider = lan_pro_group_lider.rename(columns={"Código do Assessor": "Código Assessor"})

    todos_lider = pd.concat([
        seg_group_lider,
        cam_group_lider,
        co_ter_group_lider,
        cre_group_lider,
        xpcs_group_lider,
        co_xpvp_group_lider,
        lan_pro_group_lider
    ], ignore_index=True)

    lider_final = todos_lider.groupby("Código Assessor")[["Valor Lider"]].sum().reset_index()

    atualizacoes = {
        "Valor Assessor Seguro": seg_group_lider["Valor Lider"].sum(),
        "Valor Assessor Câmbio": cam_group_lider["Valor Lider"].sum(),
        "Valor Assessor Co-Corretagem Terceiras": co_ter_group_lider["Valor Lider"].sum(),
        "Valor Assessor Crédito": cre_group_lider["Valor Lider"].sum(),
        "Valor Assessor XPCS": xpcs_group_lider["Valor Lider"].sum(),
        "Valor Assessor Co-Corretagem XPVP": co_xpvp_group_lider["Valor Lider"].sum(),
        "Valor Lançamentos Produtos": lan_pro_group_lider["Valor Lider"].sum()
    }

    for coluna, valor_soma in atualizacoes.items():
        if coluna in df_final.columns:
            df_final.loc[df_final["Código Assessor"] == "A66943", coluna] += valor_soma

    # =====================================
    # 13) Valor Total por assessor
    # =====================================
    condicao_normal = df_final['Código Assessor'] != 'A70108'
    condicao_especial = df_final['Código Assessor'] == 'A70108'

    df_final.loc[condicao_normal, "Valor Total Assessor"] = (
        df_final["Valor Assessor PJ1"]
        + df_final["Valor Assessor Seguro"]
        + df_final["Valor Assessor Câmbio"]
        + df_final["Valor Assessor Co-Corretagem Terceiras"]
        + df_final["Valor Assessor Co-Corretagem XPVP"]
        + df_final["Valor Assessor Crédito"]
        + df_final["Valor Assessor XPCS"]
        + df_final["Valor Lançamentos Manuais"]
        + df_final["Valor Lançamentos Produtos"]
    )

    df_final.loc[condicao_especial, "Valor Total Assessor"] = (
        df_final["Valor Assessor PJ1"]
        + df_final["Valor Assessor Seguro"]
        + df_final["Valor Assessor Câmbio"]
        + df_final["Valor Assessor Co-Corretagem Terceiras"]
        + df_final["Valor Assessor Co-Corretagem XPVP"]
        + df_final["Valor Assessor Crédito"]
        + df_final["Valor Assessor XPCS"]
        + df_final["Valor Lançamentos Manuais"]
        + df_final["Valor Lançamentos Produtos"]
        + soma_valor_seguros
    )

    # Remove coluna antiga "Valor Assessor" de pj1_final (se existir)
    pj1_final = pj1_final.drop(columns=["Valor Assessor"], errors="ignore")

    # =====================================
    # 14) Monta df_juntar (base detalhada)
    # =====================================
    lan_man["Valor Assessor"] = lan_man["Valor"]
    valores_debitar = lan_man['Debitar de'].dropna().unique()
    novas_linhas = []

    for codigo_debitar in valores_debitar:
        linhas_modificadas = lan_man[lan_man['Debitar de'] == codigo_debitar].copy()
        linhas_modificadas['Código'] = codigo_debitar
        linhas_modificadas['Valor Assessor'] = linhas_modificadas['Valor negativado']
        novas_linhas.append(linhas_modificadas)

    if novas_linhas:
        lan_man = pd.concat([lan_man] + novas_linhas, ignore_index=True)

    pj1_juntar = pj1_final[[
        "Cód. Assessor Direto", "Categoria", "Produto", "Cód. Cliente",
        "Receita (R$)", "Receita Líquida (R$)",
        "Repasse (%) Escritório",
        "Desconto de Transferência de Clientes Fracionado",
        "Comissão Escritório Tratada",
        "Imposto + Despesa", "Valor Imposto",
        "Sem Imposto", "percentual tratado",
        "Valor Assessor Direto"
    ]]

    seg_juntar = seg_final[[
        "Código Assessor", "Categoria", "Código Cliente",
        "Receita Bruta", "Receita Líquida",
        "Comissão (%) Escritório", "Comissão Escritório",
        "Imposto + Despesa", "Valor Imposto",
        "Sem Imposto", "Repasse Investimento Co-Corretagem Capitão",
        "Valor Assessor Seguro"
    ]]

    cam_juntar = cam_final[[
        "Código Assessor", "Categoria", "Código Cliente",
        "Receita Bruta", "Receita Líquida",
        "Comissão (%) Escritório", "Comissão Escritório",
        "Imposto + Despesa", "Valor Imposto",
        "Sem Imposto", "Repasse Investimento PJ2",
        "Valor Assessor Câmbio"
    ]]

    co_ter_juntar = co_ter_final[[
        "Código Assessor", "Categoria", "Código Cliente",
        "Receita Bruta", "Receita Líquida",
        "Comissão (%) Escritório", "Comissão Escritório",
        "Imposto + Despesa", "Valor Imposto",
        "Sem Imposto", "Repasse Investimento Co-Corretagem Assessor",
        "Valor Assessor Co-Corretagem Terceiras"
    ]]

    co_xpvp_juntar = co_xpvp_final[[
        "Código Assessor", "Categoria", "Código Cliente",
        "Receita Bruta", "Receita Líquida",
        "Comissão (%) Escritório", "Comissão Escritório",
        "Imposto + Despesa", "Valor Imposto",
        "Sem Imposto", "Repasse Investimento PJ2",
        "Valor Assessor Co-Corretagem XPVP"
    ]]

    cre_juntar = cre_final[[
        "Código Assessor", "Categoria", "Código Cliente",
        "Receita Bruta", "Receita Líquida",
        "Comissão (%) Escritório", "Comissão Escritório",
        "Imposto + Despesa", "Valor Imposto",
        "Sem Imposto", "Repasse Investimento PJ2",
        "Valor Assessor Crédito"
    ]]

    xpcs_juntar = xpcs_final[[
        "Código Assessor", "Categoria", "Código Cliente",
        "Receita Bruta", "Receita Líquida",
        "Comissão (%) Escritório", "Comissão Escritório",
        "Imposto + Despesa", "Valor Imposto",
        "Sem Imposto", "Repasse Investimento PJ2",
        "Valor Assessor XPCS"
    ]]

    lan_man_juntar = lan_man[[
        "Código", "Categoria", "Produto", "Valor Assessor"
    ]]

    lan_pro_juntar = lan_pro_filtrado[[
        "Código do Assessor", "Categoria", "Produto",
        "Cliente", "Valor Lançamentos Produtos"
    ]]

    pj1_juntar["Valor Escritório"] = (
        pj1_juntar["Repasse (%) Escritório"] * pj1_juntar["Sem Imposto"] / 100
    )
    seg_juntar["Valor Escritório"] = (
        seg_juntar["Comissão (%) Escritório"] * seg_juntar["Sem Imposto"]
    )
    cam_juntar["Valor Escritório"] = (
        cam_juntar["Comissão (%) Escritório"] * cam_juntar["Sem Imposto"]
    )
    co_ter_juntar["Valor Escritório"] = (
        co_ter_juntar["Comissão (%) Escritório"] * co_ter_juntar["Sem Imposto"]
    )
    co_xpvp_juntar["Valor Escritório"] = (
        co_xpvp_juntar["Comissão (%) Escritório"] * co_xpvp_juntar["Sem Imposto"]
    )
    cre_juntar["Valor Escritório"] = (
        cre_juntar["Comissão (%) Escritório"] * cre_juntar["Sem Imposto"]
    )
    xpcs_juntar["Valor Escritório"] = (
        xpcs_juntar["Comissão (%) Escritório"] * xpcs_juntar["Sem Imposto"]
    )

    # Apenas para conferência, se quiser
    x_total_escritorio = (
        pj1_juntar["Valor Escritório"].sum()
        + seg_juntar["Valor Escritório"].sum()
        + cam_juntar["Valor Escritório"].sum()
        + co_ter_juntar["Valor Escritório"].sum()
        + co_xpvp_juntar["Valor Escritório"].sum()
        + cre_juntar["Valor Escritório"].sum()
        + xpcs_juntar["Valor Escritório"].sum()
    )

    # Renomeia colunas para padrão comum
    pj1_juntar = pj1_juntar.rename(columns={
        "Cód. Assessor Direto": "Código Assessor",
        "Cód. Cliente": "Código Cliente",
        "Receita (R$)": "Receita Bruta",
        "Receita Líquida (R$)": "Receita Líquida",
        "Repasse (%) Escritório": "Comissão (%) Escritório",
        "Comissão Escritório Tratada": "Comissão Escritório",
        "percentual tratado": "percentual",
        "Valor Assessor Direto": "Valor Assessor"
    })
    seg_juntar = seg_juntar.rename(columns={
        "Repasse Investimento Co-Corretagem Capitão": "percentual",
        "Valor Assessor Seguro": "Valor Assessor"
    })
    cam_juntar = cam_juntar.rename(columns={
        "Repasse Investimento PJ2": "percentual",
        "Valor Assessor Câmbio": "Valor Assessor"
    })
    co_ter_juntar = co_ter_juntar.rename(columns={
        "Repasse Investimento Co-Corretagem Assessor": "percentual",
        "Valor Assessor Co-Corretagem Terceiras": "Valor Assessor"
    })
    co_xpvp_juntar = co_xpvp_juntar.rename(columns={
        "Repasse Investimento PJ2": "percentual",
        "Valor Assessor Co-Corretagem XPVP": "Valor Assessor"
    })
    cre_juntar = cre_juntar.rename(columns={
        "Repasse Investimento PJ2": "percentual",
        "Valor Assessor Crédito": "Valor Assessor"
    })
    xpcs_juntar = xpcs_juntar.rename(columns={
        "Repasse Investimento PJ2": "percentual",
        "Valor Assessor XPCS": "Valor Assessor"
    })
    lan_man_juntar = lan_man_juntar.rename(columns={
        "Código": "Código Assessor"
    })
    lan_pro_juntar = lan_pro_juntar.rename(columns={
        "Código do Assessor": "Código Assessor",
        "Valor Lançamentos Produtos": "Valor Assessor",
        "Cliente": "Código Cliente"
    })

    df_juntar = pd.concat([
        pj1_juntar,
        seg_juntar,
        cam_juntar,
        co_ter_juntar,
        co_xpvp_juntar,
        cre_juntar,
        xpcs_juntar,
        lan_man_juntar,
        lan_pro_juntar
    ], ignore_index=True)

    return df_final, df_juntar
