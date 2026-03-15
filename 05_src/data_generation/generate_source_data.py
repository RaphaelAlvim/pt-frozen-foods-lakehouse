"""
Gerador de dados sintéticos – PT Frozen Foods (Source-Oriented / Lakehouse Ready)

Gera arquivos de origem mais realistas em:
- raw/erp/
- raw/crm/
- raw/web/
- raw/reference/
- raw/weather_api/

Fontes geradas:
ERP:
- erp_pedidos.csv
- erp_itens_pedido.csv
- erp_produtos.csv
- erp_fornecedores.csv
- erp_vendedores.csv

CRM:
- crm_clientes.csv
- crm_status.csv
- crm_segmentacao.csv

WEB:
- web_event_logs.csv

REFERENCE:
- ref_calendario.csv
- ref_canais_venda.csv
- ref_localidades.csv

WEATHER_API:
- weather_porto_daily.csv

Período histórico:
2021-01-01 até 2026-02-28
"""

from pathlib import Path
from datetime import datetime, date, timedelta

import numpy as np
import pandas as pd


# ============================================================
# CONFIGURAÇÕES GERAIS
# ============================================================

RANDOM_SEED = 42
rng = np.random.default_rng(RANDOM_SEED)

START_DATE = date(2021, 1, 1)
END_DATE = date(2026, 2, 28)

N_CLIENTES = 320
BASE_PEDIDOS_DIA = 42

BASE_DIR = Path(__file__).resolve().parents[2]
RAW_DIR = BASE_DIR / "03_data" / "raw"

ERP_DIR = RAW_DIR / "erp"
CRM_DIR = RAW_DIR / "crm"
WEB_DIR = RAW_DIR / "web"
REF_DIR = RAW_DIR / "reference"
WEATHER_DIR = RAW_DIR / "weather_api"

for folder in [ERP_DIR, CRM_DIR, WEB_DIR, REF_DIR, WEATHER_DIR]:
    folder.mkdir(parents=True, exist_ok=True)


# ============================================================
# HELPERS
# ============================================================

def normalize_probs(arr):
    arr = np.array(arr, dtype=float)
    arr = np.where(arr < 0, 0, arr)
    s = arr.sum()
    if s == 0:
        return np.ones(len(arr)) / len(arr)
    return arr / s


def weighted_choice(items, weights):
    return rng.choice(items, p=normalize_probs(weights))


def clamp(x, low, high):
    return max(low, min(high, x))


def random_date_string(d: date, mode: str = "mixed") -> str:
    if mode == "mixed":
        fmt = weighted_choice(
            ["%Y-%m-%d", "%d/%m/%Y", "%Y/%m/%d", "%d-%m-%Y"],
            [0.50, 0.25, 0.15, 0.10]
        )
    elif mode == "erp":
        fmt = weighted_choice(
            ["%Y-%m-%d", "%d/%m/%Y", "%d-%m-%Y"],
            [0.55, 0.30, 0.15]
        )
    elif mode == "crm":
        fmt = weighted_choice(
            ["%Y-%m-%d", "%d/%m/%Y", "%Y/%m/%d"],
            [0.50, 0.35, 0.15]
        )
    else:
        fmt = "%Y-%m-%d"
    return d.strftime(fmt)


def add_small_duplicates(df: pd.DataFrame, frac: float = 0.01) -> pd.DataFrame:
    if df.empty or frac <= 0:
        return df
    n_dup = max(1, int(len(df) * frac))
    sample = df.sample(n=min(n_dup, len(df)), replace=False, random_state=RANDOM_SEED)
    return pd.concat([df, sample], ignore_index=True)


def add_irrelevant_columns(df: pd.DataFrame, cols: dict) -> pd.DataFrame:
    for col, values in cols.items():
        if callable(values):
            df[col] = values(len(df))
        else:
            df[col] = values
    return df


# ============================================================
# PARÂMETROS DE NEGÓCIO
# ============================================================

PREFERENCIAS_TIPO_CLIENTE = {
    "Supermercado": {
        "Peixe": 0.12, "Marisco": 0.08, "Hortícolas": 0.15, "Carne": 0.12,
        "Pré-cozinhados": 0.12, "Batatas": 0.12, "Padaria": 0.10,
        "Pastelaria": 0.07, "Sobremesas": 0.07, "Bebidas": 0.10,
        "Conservas & Secos": 0.05,
    },
    "Restaurante": {
        "Peixe": 0.25, "Marisco": 0.20, "Hortícolas": 0.15, "Carne": 0.15,
        "Pré-cozinhados": 0.10, "Batatas": 0.10, "Padaria": 0.03,
        "Pastelaria": 0.02, "Sobremesas": 0.00, "Bebidas": 0.00,
        "Conservas & Secos": 0.00,
    },
    "Hotel": {
        "Peixe": 0.18, "Marisco": 0.15, "Hortícolas": 0.12, "Carne": 0.15,
        "Pré-cozinhados": 0.10, "Batatas": 0.10, "Padaria": 0.08,
        "Pastelaria": 0.05, "Sobremesas": 0.04, "Bebidas": 0.02,
        "Conservas & Secos": 0.01,
    },
    "Take-away": {
        "Peixe": 0.08, "Marisco": 0.05, "Hortícolas": 0.08, "Carne": 0.25,
        "Pré-cozinhados": 0.20, "Batatas": 0.20, "Padaria": 0.05,
        "Pastelaria": 0.03, "Sobremesas": 0.03, "Bebidas": 0.03,
        "Conservas & Secos": 0.00,
    },
    "Particular": {
        "Peixe": 0.08, "Marisco": 0.04, "Hortícolas": 0.14, "Carne": 0.10,
        "Pré-cozinhados": 0.12, "Batatas": 0.14, "Padaria": 0.10,
        "Pastelaria": 0.10, "Sobremesas": 0.12, "Bebidas": 0.04,
        "Conservas & Secos": 0.02,
    },
}


# ============================================================
# REFERENCE
# ============================================================

def gerar_ref_canais_venda() -> pd.DataFrame:
    canais = [
        ("CH01", "E-commerce", "Pedidos realizados pelo site B2B/B2C"),
        ("CH02", "Vendas Externas", "Equipa comercial visitando clientes"),
        ("CH03", "Telefone", "Pedidos feitos por telefone"),
        ("CH04", "Marketplace", "Plataformas externas de venda"),
    ]
    return pd.DataFrame(canais, columns=["canal_id", "nome_canal", "descricao"])


def gerar_ref_localidades() -> pd.DataFrame:
    localidades = [
        ("LOC001", "Porto", "Porto", "Norte"),
        ("LOC002", "Vila Nova de Gaia", "Porto", "Norte"),
        ("LOC003", "Matosinhos", "Porto", "Norte"),
        ("LOC004", "Maia", "Porto", "Norte"),
        ("LOC005", "Braga", "Braga", "Norte"),
        ("LOC006", "Guimarães", "Braga", "Norte"),
        ("LOC007", "Viana do Castelo", "Viana do Castelo", "Norte"),
        ("LOC008", "Aveiro", "Aveiro", "Centro"),
        ("LOC009", "Espinho", "Aveiro", "Norte"),
        ("LOC010", "Póvoa de Varzim", "Porto", "Norte"),
        ("LOC011", "Santo Tirso", "Porto", "Norte"),
        ("LOC012", "Barcelos", "Braga", "Norte"),
        ("LOC013", "Valongo", "Porto", "Norte"),
    ]
    return pd.DataFrame(localidades, columns=["localidade_id", "cidade", "distrito", "regiao"])


def gerar_ref_calendario() -> pd.DataFrame:
    datas = pd.date_range(START_DATE, END_DATE, freq="D")

    nome_mes_pt = {
        1: "Janeiro", 2: "Fevereiro", 3: "Março", 4: "Abril",
        5: "Maio", 6: "Junho", 7: "Julho", 8: "Agosto",
        9: "Setembro", 10: "Outubro", 11: "Novembro", 12: "Dezembro",
    }

    nome_dia_pt = {
        0: "Segunda-feira", 1: "Terça-feira", 2: "Quarta-feira",
        3: "Quinta-feira", 4: "Sexta-feira", 5: "Sábado", 6: "Domingo",
    }

    rows = []
    for d in datas:
        weekday = d.weekday()
        rows.append({
            "data": d.date(),
            "ano": d.year,
            "mes": d.month,
            "dia": d.day,
            "trimestre": (d.month - 1) // 3 + 1,
            "nome_mes": nome_mes_pt[d.month],
            "dia_semana": weekday + 1,
            "nome_dia_semana": nome_dia_pt[weekday],
            "is_fim_de_semana": 1 if weekday >= 5 else 0,
            "is_inicio_mes": 1 if d.day <= 5 else 0,
            "is_fim_mes": 1 if d.day >= 25 else 0,
        })
    return pd.DataFrame(rows)


# ============================================================
# WEATHER API (PORTO)
# ============================================================

def gerar_weather_porto_daily(df_cal: pd.DataFrame) -> pd.DataFrame:
    temp_media_mes = {
        1: 10.0, 2: 10.8, 3: 12.4, 4: 13.4,
        5: 16.0, 6: 18.3, 7: 20.0, 8: 20.4,
        9: 19.1, 10: 16.4, 11: 12.8, 12: 10.8,
    }

    chuva_prob_mes = {
        1: 0.55, 2: 0.48, 3: 0.42, 4: 0.38,
        5: 0.30, 6: 0.18, 7: 0.10, 8: 0.12,
        9: 0.22, 10: 0.38, 11: 0.52, 12: 0.58,
    }

    rows = []
    for _, row in df_cal.iterrows():
        mes = row["mes"]
        temp_media = temp_media_mes[mes] + rng.normal(0, 1.8)
        amplitude = clamp(rng.normal(6.5, 1.2), 3.5, 9.5)

        temp_min = temp_media - amplitude / 2 + rng.normal(0, 0.6)
        temp_max = temp_media + amplitude / 2 + rng.normal(0, 0.8)

        prob_chuva = chuva_prob_mes[mes]
        choveu = 1 if rng.random() < prob_chuva else 0

        precipitacao_mm = 0.0
        if choveu:
            precipitacao_mm = round(float(clamp(rng.gamma(2.0, 3.0), 0.2, 35.0)), 1)

        rows.append({
            "data": row["data"],
            "cidade": "Porto",
            "temperatura_media": round(float(temp_media), 1),
            "temperatura_min": round(float(temp_min), 1),
            "temperatura_max": round(float(temp_max), 1),
            "choveu": choveu,
            "precipitacao_mm": precipitacao_mm,
            "humidade_media": int(clamp(rng.normal(74 if choveu else 66, 8), 45, 98)),
            "vento_kmh": round(float(clamp(rng.normal(14, 5), 2, 45)), 1),
            "fonte_api": "synthetic_weather_api_v1",
        })

    return pd.DataFrame(rows)


# ============================================================
# ERP
# ============================================================

def gerar_erp_fornecedores() -> pd.DataFrame:
    fornecedores = [
        ("F001", "PT Frozen Foods", "Portugal", "Ativo"),
        ("F002", "Atlântico Frio Distribuição", "Portugal", "Ativo"),
        ("F003", "PortoMar Congelados", "Portugal", "Ativo"),
        ("F004", "DouroHortícola", "Portugal", "Ativo"),
        ("F005", "Doces do Norte", "Portugal", "Ativo"),
        ("F006", "Mar & Serra Foods", "Portugal", "Ativo"),
    ]
    df = pd.DataFrame(fornecedores, columns=["fornecedor_id", "nome_fornecedor", "pais", "status_fornecedor"])
    df = add_irrelevant_columns(
        df,
        {
            "codigo_legacy": lambda n: [f"LEG-F{100+i}" for i in range(n)],
            "ultima_sincronizacao": lambda n: [datetime.now().strftime("%Y-%m-%d %H:%M:%S")] * n,
        }
    )
    return df


def gerar_erp_vendedores() -> pd.DataFrame:
    """
    Número de vendedores adequado a ~40-60 pedidos/dia com foco B2B.
    Inclui rotação natural de equipa ao longo dos anos.
    """
    nomes = [
        "João Moreira", "Ana Ribeiro", "Pedro Costa", "Carla Martins",
        "Miguel Sousa", "Rita Ferreira", "Tiago Lopes", "Mariana Silva",
        "Bruno Almeida", "Inês Carvalho", "Ricardo Pinto", "Sara Mendes",
    ]

    cidades_morada = ["Porto", "Matosinhos", "Valongo", "Vila Nova de Gaia"]

    rows = []
    for i, nome in enumerate(nomes, start=1):
        vendedor_id = f"VEND{i:03d}"

        data_adm = date(
            int(weighted_choice([2020, 2021, 2022, 2023, 2024, 2025], [0.14, 0.22, 0.22, 0.18, 0.14, 0.10])),
            int(rng.integers(1, 13)),
            int(rng.integers(1, 28)),
        )

        # algumas saídas
        if rng.random() < 0.22:
            min_saida = data_adm + timedelta(days=int(rng.integers(240, 900)))
            if min_saida < END_DATE:
                data_saida = min_saida + timedelta(days=int(rng.integers(0, 220)))
                if data_saida > END_DATE:
                    data_saida = None
            else:
                data_saida = None
        else:
            data_saida = None

        senioridade = weighted_choice(["Júnior", "Pleno", "Sénior"], [0.22, 0.48, 0.30])

        if senioridade == "Júnior":
            performance_score = rng.uniform(0.65, 0.95)
        elif senioridade == "Pleno":
            performance_score = rng.uniform(0.90, 1.20)
        else:
            performance_score = rng.uniform(1.10, 1.40)

        rows.append({
            "vendedor_id": vendedor_id,
            "nome_vendedor": nome,
            "idade": int(rng.integers(26, 56)),
            "estado_civil": weighted_choice(["Solteiro", "Casado", "União de facto", "Divorciado"], [0.30, 0.50, 0.12, 0.08]),
            "tem_filhos": weighted_choice([0, 1], [0.42, 0.58]),
            "cidade_morada": weighted_choice(cidades_morada, [0.35, 0.22, 0.13, 0.30]),
            "data_admissao": random_date_string(data_adm, "erp"),
            "data_saida": random_date_string(data_saida, "erp") if data_saida else "",
            "status_vendedor": "Inativo" if data_saida else "Ativo",
            "equipa": weighted_choice(["Norte Litoral", "Grande Porto", "Inside Sales"], [0.34, 0.38, 0.28]),
            "senioridade": senioridade,
            "performance_score": round(float(performance_score), 3),
            "nota_interna": weighted_choice(["", "", "", "carteira premium", "foco horeca", "rever objetivos"], [0.54, 0.16, 0.10, 0.08, 0.07, 0.05]),
        })

    df = pd.DataFrame(rows)
    df = add_irrelevant_columns(
        df,
        {
            "telefone_extensao": lambda n: [f"{rng.integers(100, 999)}" for _ in range(n)],
        }
    )
    return df


def gerar_erp_produtos() -> pd.DataFrame:
    produtos_base = [
        ("Peixe", "Filetes de pescada", 1000, 7.90, 4.20, "F001"),
        ("Peixe", "Lombos de salmão", 600, 11.50, 7.10, "F003"),
        ("Peixe", "Potas em anéis", 1000, 6.90, 3.95, "F003"),
        ("Peixe", "Bacalhau desfiado", 800, 8.50, 5.10, "F001"),
        ("Marisco", "Miolo de camarão 60/80", 800, 10.90, 7.20, "F003"),
        ("Marisco", "Miolo de mexilhão", 1000, 7.40, 4.25, "F002"),
        ("Marisco", "Cocktail de marisco", 1000, 9.80, 6.35, "F002"),
        ("Hortícolas", "Ervilhas", 1000, 2.20, 1.05, "F004"),
        ("Hortícolas", "Feijão verde", 1000, 2.40, 1.16, "F004"),
        ("Hortícolas", "Legumes para sopa", 1000, 2.60, 1.24, "F004"),
        ("Hortícolas", "Mistura chinesa", 1000, 3.40, 1.92, "F004"),
        ("Carne", "Hambúrguer bovino", 800, 5.90, 3.32, "F006"),
        ("Carne", "Nuggets de frango", 1000, 5.20, 2.84, "F006"),
        ("Carne", "Almôndegas de novilho", 1000, 6.10, 3.58, "F006"),
        ("Pré-cozinhados", "Lasanha bolonhesa", 1000, 6.90, 4.25, "F001"),
        ("Pré-cozinhados", "Empadão de carne", 1200, 7.10, 4.30, "F001"),
        ("Pré-cozinhados", "Bacalhau com natas", 1200, 8.90, 5.65, "F001"),
        ("Batatas", "Batata palito 10mm", 2500, 4.60, 2.30, "F002"),
        ("Batatas", "Batata rústica", 2500, 4.90, 2.45, "F002"),
        ("Batatas", "Batata smile", 1500, 4.80, 2.42, "F002"),
        ("Padaria", "Pão de água", 2400, 3.90, 1.78, "F005"),
        ("Padaria", "Pão de cereais", 2000, 4.40, 2.10, "F005"),
        ("Padaria", "Pão de forma", 800, 3.10, 1.42, "F005"),
        ("Pastelaria", "Croissant manteiga", 1500, 7.50, 3.95, "F005"),
        ("Pastelaria", "Pastel de nata cru", 1200, 6.90, 3.65, "F005"),
        ("Sobremesas", "Gelado baunilha 2L", 2000, 5.90, 2.95, "F001"),
        ("Sobremesas", "Gelado chocolate 2L", 2000, 5.90, 2.92, "F001"),
        ("Sobremesas", "Gelado morango 2L", 2000, 5.90, 2.90, "F001"),
        ("Sobremesas", "Tarte de maçã", 1200, 7.90, 4.05, "F005"),
        ("Sobremesas", "Tarte de amêndoa", 1200, 8.20, 4.30, "F005"),
        ("Bebidas", "Sumo de laranja 1L", 1000, 1.80, 0.92, "F006"),
        ("Bebidas", "Ice tea limão 1,5L", 1500, 1.70, 0.86, "F006"),
        ("Bebidas", "Smoothie frutos vermelhos", 750, 3.40, 1.72, "F006"),
        ("Conservas & Secos", "Arroz agulha 1kg", 1000, 1.30, 0.74, "F004"),
        ("Conservas & Secos", "Ervilhas em lata", 400, 0.90, 0.45, "F004"),
        ("Conservas & Secos", "Atum em óleo", 120, 1.10, 0.58, "F003"),
    ]

    marcas_por_fornecedor = {
        "F001": ["PT Frozen Foods", "Atlântico Select"],
        "F002": ["FrioMax", "OceanFresh"],
        "F003": ["Mar Azul", "Norte Mar"],
        "F004": ["Campo Verde", "Horta Fácil"],
        "F005": ["Doce Norte", "Padaria Premium"],
        "F006": ["Chef Express", "Mesa Pronta"],
    }

    rows = []
    for i, (cat, nome, peso, preco_base, custo_base, forn_id) in enumerate(produtos_base, start=1):
        produto_id = f"P{i:03d}"

        ano_lanc = int(weighted_choice([2021, 2022, 2023, 2024], [0.20, 0.35, 0.30, 0.15]))
        mes_lanc = int(rng.integers(1, 13))
        dia_lanc = int(rng.integers(1, 28))
        data_lancamento = date(ano_lanc, mes_lanc, dia_lanc)

        if rng.random() < 0.16:
            min_end = data_lancamento + timedelta(days=int(rng.integers(240, 650)))
            if min_end < END_DATE:
                data_fim = min_end + timedelta(days=int(rng.integers(0, 250)))
                if data_fim > END_DATE:
                    data_fim = None
            else:
                data_fim = None
        else:
            data_fim = None

        rows.append({
            "produto_id": produto_id,
            "produto_nome": f"{nome} {peso//1000}kg" if peso % 1000 == 0 else f"{nome} {peso}g",
            "categoria": cat,
            "marca": weighted_choice(marcas_por_fornecedor[forn_id], [0.7, 0.3]),
            "peso_gramas": peso,
            "preco_lista_base": round(preco_base, 2),
            "custo_base_unitario": round(custo_base, 2),
            "fornecedor_id": forn_id,
            "data_lancamento": random_date_string(data_lancamento, "erp"),
            "data_fim": random_date_string(data_fim, "erp") if data_fim else "",
            "status_produto": "Inativo" if data_fim else "Ativo",
            "popularidade_base": round(float(np.exp(rng.normal(0.0, 0.85))), 3),
            "sensibilidade_promocao": round(float(clamp(rng.normal(1.0, 0.30), 0.45, 1.85)), 3),
            "fator_sazonal_proprio": round(float(clamp(rng.normal(1.0, 0.18), 0.70, 1.40)), 3),
        })

    df = pd.DataFrame(rows)
    df = add_irrelevant_columns(
        df,
        {
            "codigo_barra_legacy": lambda n: [f"56000{rng.integers(100000, 999999)}" for _ in range(n)],
            "observacao_interna": lambda n: rng.choice(["", "", "", "rever ficha", "sku antigo"], size=n),
        }
    )
    return df


# ============================================================
# CRM
# ============================================================

def gerar_crm_clientes(ref_localidades: pd.DataFrame, ref_canais: pd.DataFrame):
    tipos = ["Supermercado", "Restaurante", "Hotel", "Take-away", "Particular"]
    pesos_tipo = [0.18, 0.34, 0.10, 0.18, 0.20]

    canais = ref_canais["nome_canal"].tolist()
    cidades = ref_localidades[["cidade", "distrito"]].copy()

    clientes_rows = []
    status_rows = []
    segment_rows = []

    start_ts = datetime.combine(START_DATE, datetime.min.time())
    end_reg_ts = datetime(2025, 10, 31)

    for i in range(1, N_CLIENTES + 1):
        cliente_id = f"C{i:04d}"
        tipo = weighted_choice(tipos, pesos_tipo)

        if tipo == "Supermercado":
            segmento = weighted_choice(["Pequeno", "Médio", "Grande"], [0.25, 0.45, 0.30])
        elif tipo == "Restaurante":
            segmento = weighted_choice(["Pequeno", "Médio", "Grande"], [0.56, 0.34, 0.10])
        elif tipo == "Hotel":
            segmento = weighted_choice(["Pequeno", "Médio", "Grande"], [0.18, 0.46, 0.36])
        elif tipo == "Take-away":
            segmento = weighted_choice(["Pequeno", "Médio", "Grande"], [0.70, 0.25, 0.05])
        else:
            segmento = "Pequeno"

        canal_origem = weighted_choice(canais, [0.34, 0.30, 0.18, 0.18])

        cidade_row = cidades.sample(1, random_state=int(rng.integers(0, 1_000_000))).iloc[0]
        cidade = cidade_row["cidade"]
        distrito = cidade_row["distrito"]

        reg_ts = start_ts + timedelta(days=int(rng.integers(0, max((end_reg_ts - start_ts).days, 1))))
        data_registo = reg_ts.date()

        score_atividade = {
            ("Supermercado", "Grande"): rng.uniform(2.2, 3.5),
            ("Supermercado", "Médio"): rng.uniform(1.4, 2.2),
            ("Supermercado", "Pequeno"): rng.uniform(0.8, 1.4),
            ("Restaurante", "Grande"): rng.uniform(1.6, 2.7),
            ("Restaurante", "Médio"): rng.uniform(0.9, 1.7),
            ("Restaurante", "Pequeno"): rng.uniform(0.4, 1.0),
            ("Hotel", "Grande"): rng.uniform(1.7, 2.8),
            ("Hotel", "Médio"): rng.uniform(1.0, 1.8),
            ("Hotel", "Pequeno"): rng.uniform(0.5, 1.0),
            ("Take-away", "Grande"): rng.uniform(1.2, 2.0),
            ("Take-away", "Médio"): rng.uniform(0.7, 1.4),
            ("Take-away", "Pequeno"): rng.uniform(0.35, 0.85),
            ("Particular", "Pequeno"): rng.uniform(0.08, 0.32),
        }[(tipo, segmento)]

        clientes_rows.append({
            "cliente_id": cliente_id,
            "nome_cliente": f"{tipo} {i:03d}",
            "tipo_cliente": tipo,
            "nif": str(200000000 + int(rng.integers(0, 80000000))),
            "data_registo": random_date_string(data_registo, "crm"),
            "cidade": cidade,
            "distrito": distrito,
            "canal_captacao": canal_origem,
            "score_atividade": round(float(score_atividade), 3),
            "obs_comercial": weighted_choice(["", "", "", "cliente estratégico", "follow-up trimestral"], [0.55, 0.15, 0.10, 0.10, 0.10]),
        })

        churn_base = {
            "Supermercado": 0.10,
            "Restaurante": 0.27,
            "Hotel": 0.18,
            "Take-away": 0.31,
            "Particular": 0.40,
        }[tipo]

        if segmento == "Grande":
            churn_base *= 0.65
        elif segmento == "Médio":
            churn_base *= 0.85

        anos_ate_fim = max((datetime.combine(END_DATE, datetime.min.time()) - reg_ts).days / 365.0, 0.2)
        churn_prob = clamp(churn_base * (1.15 if anos_ate_fim > 2 else 0.75), 0.04, 0.60)

        if rng.random() < churn_prob:
            min_churn = reg_ts + timedelta(days=int(rng.integers(100, 340)))
            if min_churn < datetime.combine(END_DATE, datetime.min.time()):
                churn_ts = min_churn + timedelta(days=int(rng.integers(0, 320)))
                if churn_ts.date() > END_DATE:
                    churn_ts = datetime.combine(END_DATE, datetime.min.time())
                status = "Inativo"
                data_status = churn_ts.date()
            else:
                status = "Ativo"
                data_status = END_DATE
        else:
            status = weighted_choice(["Ativo", "Ativo", "Ativo", "Dormante"], [0.60, 0.15, 0.10, 0.15])
            data_status = END_DATE

        status_rows.append({
            "cliente_id": cliente_id,
            "status_cliente": status,
            "data_status": random_date_string(data_status, "crm"),
            "motivo_status": weighted_choice(
                ["", "", "sem compra recente", "encerramento", "mudança de fornecedor", "baixo volume"],
                [0.45, 0.20, 0.18, 0.04, 0.07, 0.06]
            ),
        })

        segment_rows.append({
            "cliente_id": cliente_id,
            "segmento": segmento,
            "potencial_valor": weighted_choice(["Baixo", "Médio", "Alto"], [0.45, 0.38, 0.17]),
            "cluster_comercial": weighted_choice(["Cluster A", "Cluster B", "Cluster C", "Cluster D"], [0.20, 0.35, 0.30, 0.15]),
        })

    df_clientes = pd.DataFrame(clientes_rows)
    df_status = pd.DataFrame(status_rows)
    df_segment = pd.DataFrame(segment_rows)

    df_clientes = add_small_duplicates(df_clientes, 0.008)
    df_status = add_small_duplicates(df_status, 0.006)

    return df_clientes, df_status, df_segment


# ============================================================
# MAPA CLIENTE x PRODUTO
# ============================================================

def criar_mapa_produtos_clientes(df_prod: pd.DataFrame, df_clientes: pd.DataFrame):
    clientes_ids = df_clientes.drop_duplicates("cliente_id")["cliente_id"].tolist()
    tipo_por_cliente = df_clientes.drop_duplicates("cliente_id").set_index("cliente_id")["tipo_cliente"].to_dict()
    score_por_cliente = df_clientes.drop_duplicates("cliente_id").set_index("cliente_id")["score_atividade"].to_dict()

    mapa_clientes_para_produtos = {cid: [] for cid in clientes_ids}

    for _, prod in df_prod.drop_duplicates("produto_id").iterrows():
        pid = prod["produto_id"]
        cat = prod["categoria"]
        pop = prod["popularidade_base"]

        if cat in ["Batatas", "Hortícolas", "Conservas & Secos"]:
            min_cov, max_cov = 0.24, 0.78
        elif cat in ["Peixe", "Marisco", "Carne", "Pré-cozinhados"]:
            min_cov, max_cov = 0.15, 0.54
        else:
            min_cov, max_cov = 0.08, 0.38

        target_cov = clamp(rng.uniform(min_cov, max_cov) * clamp(0.75 + np.log1p(pop), 0.7, 1.35), 0.05, 0.85)
        n_clients = max(5, int(round(target_cov * len(clientes_ids))))

        pesos = []
        for cid in clientes_ids:
            tipo = tipo_por_cliente[cid]
            score = score_por_cliente[cid]
            pref_cat = PREFERENCIAS_TIPO_CLIENTE.get(tipo, {}).get(cat, 0.01)
            pesos.append((pref_cat + 0.01) * (0.65 + score))

        escolhidos = rng.choice(clientes_ids, size=min(n_clients, len(clientes_ids)), replace=False, p=normalize_probs(pesos))
        for cid in escolhidos:
            mapa_clientes_para_produtos[cid].append(pid)

    return mapa_clientes_para_produtos


# ============================================================
# APOIO VENDEDORES
# ============================================================

def preparar_vendedores_ativos(df_vendedores: pd.DataFrame):
    vend = df_vendedores.copy()
    vend["data_admissao_parsed"] = pd.to_datetime(vend["data_admissao"], dayfirst=True, errors="coerce")
    vend["data_saida_parsed"] = pd.to_datetime(vend["data_saida"], dayfirst=True, errors="coerce")
    return vend


def vendedores_ativos_no_dia(df_vendedores: pd.DataFrame, data_dia: date) -> pd.DataFrame:
    ativos = df_vendedores[
        (df_vendedores["data_admissao_parsed"].dt.date <= data_dia)
        & (
            df_vendedores["data_saida_parsed"].isna()
            | (df_vendedores["data_saida_parsed"].dt.date >= data_dia)
        )
    ].copy()
    return ativos


# ============================================================
# FATO ERP: PEDIDOS E ITENS
# ============================================================

def fator_sazonal_categoria(mes: int, categoria: str) -> float:
    base = {
        1: 0.92, 2: 0.94, 3: 0.99, 4: 1.03, 5: 1.08, 6: 1.15,
        7: 1.22, 8: 1.18, 9: 1.05, 10: 0.98, 11: 1.10, 12: 1.26,
    }[mes]

    if categoria == "Sobremesas":
        if mes in [6, 7, 8]:
            base *= 1.55
        elif mes == 12:
            base *= 1.30
        else:
            base *= 0.82
    elif categoria in ["Pré-cozinhados", "Carne"]:
        if mes in [11, 12, 1, 2]:
            base *= 1.18
        elif mes in [7, 8]:
            base *= 0.90
    elif categoria == "Batatas":
        if mes in [6, 7, 8]:
            base *= 1.24
    elif categoria == "Marisco":
        if mes == 12:
            base *= 1.35
    elif categoria == "Bebidas":
        if mes in [6, 7, 8]:
            base *= 1.20

    return base


def fator_macro_dia(data_dia: date) -> float:
    ano = data_dia.year
    mes = data_dia.month
    weekday = datetime.combine(data_dia, datetime.min.time()).weekday()

    trend_ano = {
        2021: 0.94,
        2022: 1.00,
        2023: 1.08,
        2024: 1.17,
        2025: 1.25,
        2026: 1.30,
    }[ano]

    saz_mes = {
        1: 0.93, 2: 0.96, 3: 1.00, 4: 1.03, 5: 1.08, 6: 1.14,
        7: 1.20, 8: 1.16, 9: 1.04, 10: 0.99, 11: 1.12, 12: 1.26,
    }[mes]

    if weekday <= 3:
        fator_semana = 1.05
    elif weekday == 4:
        fator_semana = 1.00
    elif weekday == 5:
        fator_semana = 0.84
    else:
        fator_semana = 0.64

    fator = trend_ano * saz_mes * fator_semana

    if ano == 2022 and mes in [3, 4]:
        fator *= 0.93
    if ano == 2024 and mes in [5, 6]:
        fator *= 1.07
    if mes in [11, 12]:
        fator *= 1.05

    fator *= clamp(rng.normal(1.0, 0.09), 0.78, 1.28)
    return fator


def escolher_vendedor_para_pedido(canal_nome: str, cli: pd.Series, vendedores_ativos_df: pd.DataFrame):
    """
    Define quando um pedido tem vendedor responsável.
    """
    if vendedores_ativos_df.empty:
        return ""

    # probabilidade por canal
    prob_vendedor = {
        "Vendas Externas": 0.98,
        "Telefone": 0.72,
        "E-commerce": 0.18 if cli["tipo_cliente"] in ["Supermercado", "Restaurante", "Hotel"] else 0.04,
        "Marketplace": 0.01,
    }[canal_nome]

    if rng.random() > prob_vendedor:
        return ""

    vend = vendedores_ativos_df.copy()

    # equipas e carteiras mais coerentes
    peso_equipa = np.ones(len(vend), dtype=float)

    if canal_nome == "Telefone":
        peso_equipa *= np.where(vend["equipa"] == "Inside Sales", 1.45, 0.85)
    elif canal_nome == "Vendas Externas":
        peso_equipa *= np.where(vend["equipa"].isin(["Grande Porto", "Norte Litoral"]), 1.20, 0.75)
    elif canal_nome == "E-commerce":
        peso_equipa *= np.where(vend["equipa"] == "Inside Sales", 1.25, 0.85)

    # vendedores com mais performance recebem mais pedidos
    pesos = vend["performance_score"].values * peso_equipa
    idx = rng.choice(vend.index, p=normalize_probs(pesos))
    return vend.loc[idx, "vendedor_id"]


def gerar_erp_pedidos_itens(
    crm_clientes: pd.DataFrame,
    crm_status: pd.DataFrame,
    erp_produtos: pd.DataFrame,
    erp_vendedores: pd.DataFrame,
    ref_canais: pd.DataFrame,
    weather_daily: pd.DataFrame,
    mapa_clientes_para_produtos: dict
):
    clientes_base = crm_clientes.drop_duplicates("cliente_id").copy()
    status_base = crm_status.drop_duplicates("cliente_id").copy()
    clientes_full = clientes_base.merge(status_base[["cliente_id", "status_cliente", "data_status"]], on="cliente_id", how="left")

    produtos_base = erp_produtos.drop_duplicates("produto_id").copy()
    vendedores_prepared = preparar_vendedores_ativos(erp_vendedores)

    weather_map = weather_daily.set_index("data").to_dict("index")
    canal_id_por_nome = ref_canais.set_index("nome_canal")["canal_id"].to_dict()

    pedidos_rows = []
    itens_rows = []

    pedido_counter = 1
    item_counter = 1

    datas = pd.date_range(START_DATE, END_DATE, freq="D")

    for d in datas:
        data_dia = d.date()
        weather = weather_map[data_dia]

        ativos = clientes_full.copy()
        ativos["data_registo_parsed"] = pd.to_datetime(clientes_base["data_registo"], dayfirst=True, errors="coerce")
        ativos = ativos[ativos["data_registo_parsed"].dt.date <= data_dia]

        ativos["peso_status"] = np.where(ativos["status_cliente"] == "Ativo", 1.0, np.where(ativos["status_cliente"] == "Dormante", 0.28, 0.05))
        ativos = ativos[ativos["peso_status"] > 0.04]

        if ativos.empty:
            continue

        vendedores_ativos_df = vendedores_ativos_no_dia(vendedores_prepared, data_dia)

        expected_pedidos = BASE_PEDIDOS_DIA * fator_macro_dia(data_dia)

        if weather["choveu"] == 1:
            expected_pedidos *= 0.97
            expected_pedidos *= 1.06 if data_dia.month in [11, 12, 1, 2] else 1.0
        else:
            expected_pedidos *= 1.02 if data_dia.month in [6, 7, 8] else 1.0

        n_pedidos = int(max(0, rng.poisson(expected_pedidos)))

        ativos["peso_pick"] = normalize_probs(ativos["score_atividade"].values * ativos["peso_status"].values)

        for _ in range(n_pedidos):
            cli_idx = rng.choice(ativos.index, p=ativos["peso_pick"].values)
            cli = ativos.loc[cli_idx]

            canal_weights = {
                "E-commerce": 0.16,
                "Vendas Externas": 0.34,
                "Telefone": 0.22,
                "Marketplace": 0.08,
            }

            if data_dia.year >= 2023:
                canal_weights["E-commerce"] += 0.04
            if data_dia.year >= 2024:
                canal_weights["E-commerce"] += 0.04
                canal_weights["Marketplace"] += 0.03
                canal_weights["Telefone"] -= 0.03
            if data_dia.year >= 2025:
                canal_weights["E-commerce"] += 0.03
                canal_weights["Marketplace"] += 0.03
                canal_weights["Vendas Externas"] -= 0.02

            if cli["tipo_cliente"] == "Particular":
                canal_weights["E-commerce"] += 0.12
                canal_weights["Marketplace"] += 0.08
                canal_weights["Vendas Externas"] -= 0.15
            elif cli["tipo_cliente"] in ["Supermercado", "Hotel"]:
                canal_weights["Vendas Externas"] += 0.10
            elif cli["tipo_cliente"] == "Restaurante":
                canal_weights["Telefone"] += 0.05

            canal_nome = weighted_choice(list(canal_weights.keys()), list(canal_weights.values()))
            canal_id = canal_id_por_nome[canal_nome]

            vendedor_id = escolher_vendedor_para_pedido(canal_nome, cli, vendedores_ativos_df)

            pedido_id = f"PED{data_dia.strftime('%Y%m%d')}{pedido_counter:05d}"
            pedido_counter += 1

            n_itens = int(clamp(rng.poisson(1.15 + cli["score_atividade"] * 0.85), 1, 8))

            pedidos_rows.append({
                "pedido_id": pedido_id,
                "data_pedido": random_date_string(data_dia, "erp"),
                "cliente_id": cli["cliente_id"],
                "canal_id": canal_id,
                "vendedor_id": vendedor_id,
                "cidade_entrega": cli["cidade"],
                "estado_pedido": weighted_choice(["Entregue", "Entregue", "Entregue", "Parcial"], [0.72, 0.18, 0.06, 0.04]),
                "prazo_entrega_dias": int(clamp(round(rng.normal(2.6, 1.0)), 1, 7)),
                "observacao_pedido": weighted_choice(["", "", "", "janela AM", "urgente", "confirmar stock"], [0.52, 0.18, 0.10, 0.08, 0.07, 0.05]),
            })

            produtos_cliente_ids = mapa_clientes_para_produtos.get(cli["cliente_id"], [])
            candidatos = produtos_base[produtos_base["produto_id"].isin(produtos_cliente_ids)].copy()
            if candidatos.empty:
                candidatos = produtos_base.copy()

            for _ in range(n_itens):
                prefs = PREFERENCIAS_TIPO_CLIENTE.get(cli["tipo_cliente"], {})
                categorias_disp = candidatos["categoria"].unique().tolist()
                cats = [c for c in categorias_disp if prefs.get(c, 0) > 0]
                if cats:
                    cat_escolhida = weighted_choice(cats, [prefs[c] for c in cats])
                    subset = candidatos[candidatos["categoria"] == cat_escolhida].copy()
                else:
                    subset = candidatos.copy()

                if subset.empty:
                    subset = candidatos.copy()

                probs = normalize_probs(subset["popularidade_base"].values)
                prod = subset.loc[rng.choice(subset.index, p=probs)]

                categoria = prod["categoria"]
                saz = fator_sazonal_categoria(data_dia.month, categoria) * float(prod["fator_sazonal_proprio"])

                base_qtd = {
                    "Supermercado": rng.gamma(2.4, 1.3),
                    "Restaurante": rng.gamma(2.0, 1.2),
                    "Hotel": rng.gamma(2.2, 1.3),
                    "Take-away": rng.gamma(1.6, 1.0),
                    "Particular": rng.gamma(1.2, 0.9),
                }[cli["tipo_cliente"]]

                if cli["score_atividade"] > 2.0:
                    base_qtd *= 1.25
                if categoria in ["Conservas & Secos", "Batatas", "Hortícolas"]:
                    base_qtd *= 1.12
                if categoria in ["Peixe", "Marisco"]:
                    base_qtd *= 0.95

                quantidade = max(1, int(round(base_qtd * saz * clamp(rng.normal(1.0, 0.16), 0.55, 1.55))))

                inflacao_ano = {2021: 0.98, 2022: 1.00, 2023: 1.04, 2024: 1.08, 2025: 1.11, 2026: 1.13}[data_dia.year]
                preco_lista_dia = float(prod["preco_lista_base"]) * inflacao_ano * clamp(rng.normal(1.0, 0.025), 0.95, 1.08)

                desconto_pct = 0.0
                if data_dia.month in [11, 12]:
                    desconto_pct += 0.04
                if data_dia.month in [6, 7, 8] and categoria in ["Sobremesas", "Batatas", "Bebidas"]:
                    desconto_pct += 0.05
                if cli["tipo_cliente"] == "Supermercado":
                    desconto_pct += 0.02
                if cli["score_atividade"] > 2.2:
                    desconto_pct += 0.02
                if vendedor_id:
                    desconto_pct += weighted_choice([0.00, 0.01, 0.02], [0.55, 0.30, 0.15])

                if rng.random() < 0.48 * float(prod["sensibilidade_promocao"]):
                    desconto_pct += weighted_choice([0.00, 0.03, 0.05, 0.08, 0.12], [0.25, 0.25, 0.24, 0.18, 0.08])

                desconto_pct = clamp(desconto_pct, 0.0, 0.22)
                desconto_unit = round(preco_lista_dia * desconto_pct, 2)
                preco_unit = round(max(preco_lista_dia - desconto_unit, 0.10), 2)

                custo_unit = round(float(prod["custo_base_unitario"]) * inflacao_ano * clamp(rng.normal(1.0, 0.03), 0.94, 1.10), 2)

                produto_id_final = prod["produto_id"]
                if rng.random() < 0.004:
                    produto_id_final = ""

                itens_rows.append({
                    "item_pedido_id": f"IT{item_counter:09d}",
                    "pedido_id": pedido_id,
                    "produto_id": produto_id_final,
                    "quantidade": quantidade,
                    "preco_lista_unitario": round(preco_lista_dia, 2),
                    "desconto_unitario": desconto_unit,
                    "preco_venda_unitario": preco_unit,
                    "custo_unitario": custo_unit,
                    "lote_fornecedor": f"L{rng.integers(10000, 99999)}",
                    "flag_promocao": 1 if desconto_pct >= 0.05 else 0,
                    "nota_item": weighted_choice(["", "", "", "troca embalagem", "validade curta"], [0.60, 0.18, 0.10, 0.07, 0.05]),
                })
                item_counter += 1

    df_pedidos = pd.DataFrame(pedidos_rows)
    df_itens = pd.DataFrame(itens_rows)

    df_pedidos = add_small_duplicates(df_pedidos, 0.008)
    df_itens = add_small_duplicates(df_itens, 0.006)

    df_pedidos = add_irrelevant_columns(
        df_pedidos,
        {
            "sistema_origem": "ERP_FROZEN_V2",
            "usuario_ultima_alteracao": lambda n: rng.choice(["svc_sync", "maria.s", "joao.p", "ana.c"], size=n),
        }
    )

    return df_pedidos, df_itens


# ============================================================
# WEB
# ============================================================

def termos_busca_por_categoria():
    return {
        "Peixe": ["pescada", "bacalhau", "salmão", "filetes"],
        "Marisco": ["camarão", "mexilhão", "marisco", "cocktail marisco"],
        "Hortícolas": ["ervilhas", "legumes", "sopa", "mistura chinesa"],
        "Carne": ["hambúrguer", "nuggets", "almôndegas", "frango"],
        "Pré-cozinhados": ["lasanha", "empadão", "bacalhau natas", "refeição pronta"],
        "Batatas": ["batata palito", "batata rustica", "batata smile", "batatas"],
        "Padaria": ["pão", "pão cereais", "pão forma"],
        "Pastelaria": ["croissant", "pastel nata", "pastelaria"],
        "Sobremesas": ["gelado", "tarte", "sobremesa", "gelado chocolate"],
        "Bebidas": ["sumo", "ice tea", "smoothie"],
        "Conservas & Secos": ["arroz", "atum", "ervilhas lata"],
    }


def gerar_web_event_logs(erp_produtos: pd.DataFrame, crm_clientes: pd.DataFrame, crm_status: pd.DataFrame, weather_daily: pd.DataFrame):
    produtos = erp_produtos.drop_duplicates("produto_id").copy()
    clientes = crm_clientes.drop_duplicates("cliente_id").copy()
    status = crm_status.drop_duplicates("cliente_id").copy()
    clientes = clientes.merge(status[["cliente_id", "status_cliente"]], on="cliente_id", how="left")

    weather_map = weather_daily.set_index("data").to_dict("index")
    termos_cat = termos_busca_por_categoria()

    dispositivos = ["Desktop", "Mobile", "Tablet"]

    event_id = 1
    rows = []

    pop_probs = normalize_probs(produtos["popularidade_base"].values)

    datas = pd.date_range(START_DATE, END_DATE, freq="D")
    for d in datas:
        data_dia = d.date()
        weather = weather_map[data_dia]

        base_sessoes = 58
        saz_mes = {
            1: 0.92, 2: 0.95, 3: 0.99, 4: 1.03, 5: 1.08, 6: 1.16,
            7: 1.23, 8: 1.19, 9: 1.05, 10: 0.99, 11: 1.10, 12: 1.23,
        }[data_dia.month]
        trend_ano = {2021: 0.94, 2022: 1.00, 2023: 1.08, 2024: 1.18, 2025: 1.28, 2026: 1.34}[data_dia.year]
        weekday = d.weekday()
        fator_semana = 0.86 if weekday == 5 else 0.80 if weekday == 6 else 1.0

        sessions = base_sessoes * saz_mes * trend_ano * fator_semana

        if data_dia.month in [11, 12]:
            sessions *= 1.10
        if weather["choveu"] == 1 and data_dia.month in [11, 12, 1, 2]:
            sessions *= 1.05

        sessions *= clamp(rng.normal(1.0, 0.10), 0.80, 1.30)
        n_sessoes = int(max(0, rng.poisson(sessions)))

        clientes_validos = clientes.copy()

        for s in range(n_sessoes):
            sessao_id = f"S{data_dia.strftime('%Y%m%d')}{s:05d}"

            prob_identificado = 0.52 if weekday <= 4 else 0.42
            if data_dia.year >= 2024:
                prob_identificado += 0.04

            if rng.random() < prob_identificado and not clientes_validos.empty:
                probs_cli = normalize_probs(clientes_validos["score_atividade"].values)
                cli_idx = rng.choice(clientes_validos.index, p=probs_cli)
                cli = clientes_validos.loc[cli_idx]
                cliente_id = cli["cliente_id"]
                visitante_id = ""
                tipo_cliente = cli["tipo_cliente"]
                score_atividade = cli["score_atividade"]
            else:
                cliente_id = ""
                visitante_id = f"V{rng.integers(1000000, 9999999)}"
                tipo_cliente = None
                score_atividade = 0.15

            origem_weights = {
                "Orgânico": 0.38,
                "Email": 0.15,
                "Anúncio": 0.24,
                "Direto": 0.18,
                "Social": 0.05,
            }
            if data_dia.year >= 2024:
                origem_weights["Social"] += 0.03
                origem_weights["Direto"] -= 0.02
            if data_dia.month in [11, 12]:
                origem_weights["Email"] += 0.05
                origem_weights["Anúncio"] += 0.04

            origem = weighted_choice(list(origem_weights.keys()), list(origem_weights.values()))

            if tipo_cliente in ["Supermercado", "Hotel"]:
                dispositivo = weighted_choice(dispositivos, [0.55, 0.35, 0.10])
            elif tipo_cliente == "Particular" or origem in ["Social", "Anúncio"]:
                dispositivo = weighted_choice(dispositivos, [0.26, 0.64, 0.10])
            else:
                dispositivo = weighted_choice(dispositivos, [0.42, 0.48, 0.10])

            base_eventos = 2.4 + score_atividade * 1.7
            if origem in ["Email", "Direto"]:
                base_eventos += 0.8
            if dispositivo == "Mobile":
                base_eventos -= 0.2

            n_eventos = int(clamp(rng.poisson(base_eventos), 2, 12))

            current_time = datetime(data_dia.year, data_dia.month, data_dia.day, int(rng.integers(8, 22)), int(rng.integers(0, 60)), int(rng.integers(0, 60)))

            viewed = []

            if rng.random() < (0.20 if origem in ["Orgânico", "Anúncio"] else 0.10):
                cat_search = weighted_choice(list(termos_cat.keys()), produtos.groupby("categoria")["popularidade_base"].sum().reindex(list(termos_cat.keys()), fill_value=1).values)
                termo = weighted_choice(termos_cat[cat_search], np.ones(len(termos_cat[cat_search])))
                rows.append({
                    "evento_id": event_id,
                    "data_hora": current_time,
                    "sessao_id": sessao_id,
                    "cliente_id": cliente_id,
                    "visitante_id": visitante_id,
                    "produto_id": "",
                    "tipo_evento": "search",
                    "valor_busca": termo,
                    "dispositivo": dispositivo,
                    "origem_trafego": origem,
                })
                event_id += 1
                current_time += timedelta(seconds=int(rng.integers(15, 160)))

            n_views = max(1, int(round(n_eventos * rng.uniform(0.45, 0.75))))
            for _ in range(n_views):
                prod = produtos.loc[rng.choice(produtos.index, p=pop_probs)]
                viewed.append(prod["produto_id"])

                pid = prod["produto_id"]
                if rng.random() < 0.003:
                    pid = ""

                rows.append({
                    "evento_id": event_id,
                    "data_hora": current_time,
                    "sessao_id": sessao_id,
                    "cliente_id": cliente_id,
                    "visitante_id": visitante_id,
                    "produto_id": pid,
                    "tipo_evento": "view",
                    "valor_busca": "",
                    "dispositivo": dispositivo,
                    "origem_trafego": origem,
                })
                event_id += 1
                current_time += timedelta(seconds=int(rng.integers(10, 240)))

            add_prob = 0.17 + (0.08 if cliente_id else 0.0) + (0.04 if origem in ["Email", "Direto"] else 0.0)
            purchase_prob = 0.08 + (0.07 if cliente_id else 0.0) + (0.03 if origem in ["Email", "Direto"] else 0.0)

            if data_dia.month in [11, 12]:
                add_prob += 0.04
                purchase_prob += 0.03

            viewed_unique = list(pd.unique(viewed))
            added = []

            if viewed_unique and rng.random() < add_prob:
                n_adds = int(clamp(rng.poisson(1.1), 1, min(len(viewed_unique), 3)))
                chosen_adds = rng.choice(viewed_unique, size=n_adds, replace=False)
                for pid in chosen_adds:
                    added.append(pid)
                    rows.append({
                        "evento_id": event_id,
                        "data_hora": current_time,
                        "sessao_id": sessao_id,
                        "cliente_id": cliente_id,
                        "visitante_id": visitante_id,
                        "produto_id": pid,
                        "tipo_evento": "add_to_cart",
                        "valor_busca": "",
                        "dispositivo": dispositivo,
                        "origem_trafego": origem,
                    })
                    event_id += 1
                    current_time += timedelta(seconds=int(rng.integers(15, 220)))

            if added and rng.random() < purchase_prob:
                n_purchases = int(clamp(rng.poisson(1.0), 1, min(len(added), 2)))
                chosen_purchases = rng.choice(added, size=n_purchases, replace=False)
                for pid in chosen_purchases:
                    rows.append({
                        "evento_id": event_id,
                        "data_hora": current_time,
                        "sessao_id": sessao_id,
                        "cliente_id": cliente_id,
                        "visitante_id": visitante_id,
                        "produto_id": pid,
                        "tipo_evento": "purchase",
                        "valor_busca": "",
                        "dispositivo": dispositivo,
                        "origem_trafego": origem,
                    })
                    event_id += 1
                    current_time += timedelta(seconds=int(rng.integers(15, 180)))

            eventos_restantes = max(0, n_eventos - (1 if any(r["sessao_id"] == sessao_id and r["tipo_evento"] == "search" for r in rows[-5:]) else 0) - n_views - len(added))
            for _ in range(eventos_restantes):
                tipo = weighted_choice(["view", "search"], [0.74, 0.26])
                if tipo == "view":
                    prod = produtos.loc[rng.choice(produtos.index, p=pop_probs)]
                    pid = prod["produto_id"]
                    valor_busca = ""
                else:
                    cat_search = weighted_choice(list(termos_cat.keys()), produtos.groupby("categoria")["popularidade_base"].sum().reindex(list(termos_cat.keys()), fill_value=1).values)
                    pid = ""
                    valor_busca = weighted_choice(termos_cat[cat_search], np.ones(len(termos_cat[cat_search])))

                rows.append({
                    "evento_id": event_id,
                    "data_hora": current_time,
                    "sessao_id": sessao_id,
                    "cliente_id": cliente_id,
                    "visitante_id": visitante_id,
                    "produto_id": pid,
                    "tipo_evento": tipo,
                    "valor_busca": valor_busca,
                    "dispositivo": dispositivo,
                    "origem_trafego": origem,
                })
                event_id += 1
                current_time += timedelta(seconds=int(rng.integers(10, 180)))

    df = pd.DataFrame(rows)
    df = add_small_duplicates(df, 0.005)
    df = add_irrelevant_columns(
        df,
        {
            "user_agent_family": lambda n: rng.choice(["Chrome", "Safari", "Edge", "Firefox"], size=n),
            "tracking_batch_id": lambda n: [f"WB{rng.integers(1000, 9999)}" for _ in range(n)],
        }
    )
    return df


# ============================================================
# MAIN
# ============================================================

def main():
    print("Gerando fontes de dados da PT Frozen Foods...")
    print(f"Período: {START_DATE} até {END_DATE}")

    ref_canais = gerar_ref_canais_venda()
    ref_localidades = gerar_ref_localidades()
    ref_calendario = gerar_ref_calendario()

    weather_daily = gerar_weather_porto_daily(ref_calendario)

    erp_fornecedores = gerar_erp_fornecedores()
    erp_vendedores = gerar_erp_vendedores()
    erp_produtos = gerar_erp_produtos()

    crm_clientes, crm_status, crm_segmentacao = gerar_crm_clientes(ref_localidades, ref_canais)

    mapa_clientes_para_produtos = criar_mapa_produtos_clientes(erp_produtos, crm_clientes)

    erp_pedidos, erp_itens = gerar_erp_pedidos_itens(
        crm_clientes=crm_clientes,
        crm_status=crm_status,
        erp_produtos=erp_produtos,
        erp_vendedores=erp_vendedores,
        ref_canais=ref_canais,
        weather_daily=weather_daily,
        mapa_clientes_para_produtos=mapa_clientes_para_produtos
    )

    web_logs = gerar_web_event_logs(
        erp_produtos=erp_produtos,
        crm_clientes=crm_clientes,
        crm_status=crm_status,
        weather_daily=weather_daily
    )

    print(f"Salvando dados em: {RAW_DIR}")

    erp_fornecedores.to_csv(ERP_DIR / "erp_fornecedores.csv", index=False)
    erp_vendedores.to_csv(ERP_DIR / "erp_vendedores.csv", index=False)
    erp_produtos.to_csv(ERP_DIR / "erp_produtos.csv", index=False)
    erp_pedidos.to_csv(ERP_DIR / "erp_pedidos.csv", index=False)
    erp_itens.to_csv(ERP_DIR / "erp_itens_pedido.csv", index=False)

    crm_clientes.to_csv(CRM_DIR / "crm_clientes.csv", index=False)
    crm_status.to_csv(CRM_DIR / "crm_status.csv", index=False)
    crm_segmentacao.to_csv(CRM_DIR / "crm_segmentacao.csv", index=False)

    web_logs.to_csv(WEB_DIR / "web_event_logs.csv", index=False)

    ref_calendario.to_csv(REF_DIR / "ref_calendario.csv", index=False)
    ref_canais.to_csv(REF_DIR / "ref_canais_venda.csv", index=False)
    ref_localidades.to_csv(REF_DIR / "ref_localidades.csv", index=False)

    weather_daily.to_csv(WEATHER_DIR / "weather_porto_daily.csv", index=False)

    print("Concluído!")
    print(f"ERP - fornecedores: {len(erp_fornecedores)}")
    print(f"ERP - vendedores: {len(erp_vendedores)}")
    print(f"ERP - produtos: {len(erp_produtos)}")
    print(f"ERP - pedidos: {len(erp_pedidos)}")
    print(f"ERP - itens: {len(erp_itens)}")
    print(f"CRM - clientes: {len(crm_clientes)}")
    print(f"CRM - status: {len(crm_status)}")
    print(f"CRM - segmentação: {len(crm_segmentacao)}")
    print(f"WEB - event logs: {len(web_logs)}")
    print(f"REF - calendário: {len(ref_calendario)}")
    print(f"WEATHER - daily: {len(weather_daily)}")


if __name__ == "__main__":
    main()