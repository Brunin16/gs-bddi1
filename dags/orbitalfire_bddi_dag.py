from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.oracle.hooks.oracle import OracleHook
from datetime import datetime

import json
import os
import urllib.request
import urllib.error

import pandas as pd

HOME     = os.environ.get("ORBITALFIRE_HOME", "/opt/airflow")
CSV_PATH = os.path.join(HOME, "data", "fire_risk_dataset.csv")
STAGING  = os.path.join(HOME, "include", "staging")
os.makedirs(STAGING, exist_ok=True)

ORACLE_CONN_ID = "oracle_fiap"

PONTOS_CLIMA = [
    {"regiao": "Amazonia (Manaus)",          "lat": -3.10,  "lon": -60.02},
    {"regiao": "Cerrado (Brasilia)",         "lat": -15.78, "lon": -47.93},
    {"regiao": "Pantanal (Corumba)",         "lat": -19.01, "lon": -57.65},
    {"regiao": "Caatinga (Petrolina)",       "lat": -9.39,  "lon": -40.50},
    {"regiao": "Mata Atlantica (Sao Paulo)", "lat": -23.55, "lon": -46.63},
]


def _classifica_risco(fwi):
    if fwi < 12: return "baixo"
    if fwi < 18: return "moderado"
    if fwi < 24: return "alto"
    return "critico"


def extrair_csv():
    df = pd.read_csv(CSV_PATH)
    df.to_csv(os.path.join(STAGING, "raw_focos.csv"), index=False)
    print(f"Extraindo dados (CSV)... {len(df)} linhas lidas de {CSV_PATH}")


def extrair_clima_api():
    registros = []
    for p in PONTOS_CLIMA:
        url = (
            "https://api.open-meteo.com/v1/forecast"
            f"?latitude={p['lat']}&longitude={p['lon']}"
            "&current=temperature_2m,relative_humidity_2m,wind_speed_10m,precipitation"
        )
        try:
            with urllib.request.urlopen(url, timeout=15) as resp:
                cur = json.loads(resp.read())["current"]
            registros.append({
                "regiao": p["regiao"], "latitude": p["lat"], "longitude": p["lon"],
                "temperatura_c": cur["temperature_2m"],
                "umidade_relativa": cur["relative_humidity_2m"],
                "vento_kmh": cur["wind_speed_10m"],
                "precipitacao_mm": cur["precipitation"],
                "fonte": "open-meteo",
            })
            print(f"Clima OK: {p['regiao']}")
        except (urllib.error.URLError, KeyError, TimeoutError) as e:
            print(f"Clima FALLBACK: {p['regiao']} ({e})")
            registros.append({
                "regiao": p["regiao"], "latitude": p["lat"], "longitude": p["lon"],
                "temperatura_c": None, "umidade_relativa": None,
                "vento_kmh": None, "precipitacao_mm": None,
                "fonte": "fallback",
            })
    with open(os.path.join(STAGING, "raw_clima.json"), "w") as f:
        json.dump(registros, f, indent=2)
    print(f"Extraindo dados (API)... {len(registros)} regioes coletadas")


def transformar():
    df    = pd.read_csv(os.path.join(STAGING, "raw_focos.csv"))
    antes = len(df)

    df = df.drop_duplicates()
    df = df.dropna(subset=["temperatura_c", "umidade_relativa", "ocorrencia_foco"])
    df["mes"]             = df["mes"].astype(int)
    df["dias_sem_chuva"]  = df["dias_sem_chuva"].astype(int)
    df["ocorrencia_foco"] = df["ocorrencia_foco"].astype(int)
    df["tipo_cobertura"]  = df["tipo_cobertura"].str.strip().str.lower()
    for col in df.select_dtypes(include="number").columns:
        if df[col].isna().any():
            df[col] = df[col].fillna(df[col].median())
    df["risco_classe"] = df["indice_fwi"].apply(_classifica_risco)
    df = df.reset_index(drop=True)
    df.insert(0, "foco_id", df.index + 1)
    df.to_csv(os.path.join(STAGING, "curated_focos.csv"), index=False)

    clima = pd.read_json(os.path.join(STAGING, "raw_clima.json"))
    clima.insert(0, "clima_id", range(1, len(clima) + 1))
    clima.to_json(os.path.join(STAGING, "curated_clima.json"), orient="records")

    print(f"Transformando dados... {antes} -> {len(df)} linhas | "
          f"risco: {df['risco_classe'].value_counts().to_dict()}")


def criar_tabelas():
    hook = OracleHook(oracle_conn_id=ORACLE_CONN_ID)
    for stmt in [
        "BEGIN EXECUTE IMMEDIATE 'DROP TABLE foco_queimada'; "
        "EXCEPTION WHEN OTHERS THEN IF SQLCODE != -942 THEN RAISE; END IF; END;",
        "BEGIN EXECUTE IMMEDIATE 'DROP TABLE clima_atual'; "
        "EXCEPTION WHEN OTHERS THEN IF SQLCODE != -942 THEN RAISE; END IF; END;",
        """CREATE TABLE foco_queimada (
            foco_id NUMBER PRIMARY KEY, mes NUMBER(2),
            temperatura_c NUMBER(5,1), umidade_relativa NUMBER(5,1),
            velocidade_vento_kmh NUMBER(5,1), precipitacao_mm NUMBER(6,1),
            dias_sem_chuva NUMBER(4), ndvi NUMBER(5,3), indice_fwi NUMBER(6,1),
            latitude NUMBER(8,4), longitude NUMBER(8,4), altitude_m NUMBER(6),
            tipo_cobertura VARCHAR2(20), ocorrencia_foco NUMBER(1),
            risco_classe VARCHAR2(10), data_carga DATE DEFAULT SYSDATE)""",
        """CREATE TABLE clima_atual (
            clima_id NUMBER PRIMARY KEY, regiao VARCHAR2(40),
            latitude NUMBER(8,4), longitude NUMBER(8,4),
            temperatura_c NUMBER(5,1), umidade_relativa NUMBER(5,1),
            vento_kmh NUMBER(5,1), precipitacao_mm NUMBER(6,1),
            fonte VARCHAR2(20), coletado_em DATE DEFAULT SYSDATE)""",
    ]:
        hook.run(stmt)
    print("Tabelas foco_queimada e clima_atual criadas no Oracle")


def carregar_oracle():
    hook = OracleHook(oracle_conn_id=ORACLE_CONN_ID)
    conn = hook.get_conn()
    cur  = conn.cursor()

    df   = pd.read_csv(os.path.join(STAGING, "curated_focos.csv"))
    cols = ["foco_id", "mes", "temperatura_c", "umidade_relativa",
            "velocidade_vento_kmh", "precipitacao_mm", "dias_sem_chuva", "ndvi",
            "indice_fwi", "latitude", "longitude", "altitude_m",
            "tipo_cobertura", "ocorrencia_foco", "risco_classe"]
    cur.execute("DELETE FROM foco_queimada")
    cur.executemany(
        f"INSERT INTO foco_queimada ({', '.join(cols)}) "
        f"VALUES ({', '.join(f':{i+1}' for i in range(len(cols)))})",
        list(df[cols].itertuples(index=False, name=None)),
    )

    c     = pd.read_json(os.path.join(STAGING, "curated_clima.json"))
    c     = c.where(pd.notna(c), None)
    ccols = ["clima_id", "regiao", "latitude", "longitude", "temperatura_c",
             "umidade_relativa", "vento_kmh", "precipitacao_mm", "fonte"]
    cur.execute("DELETE FROM clima_atual")
    cur.executemany(
        f"INSERT INTO clima_atual ({', '.join(ccols)}) "
        f"VALUES ({', '.join(f':{i+1}' for i in range(len(ccols)))})",
        list(c[ccols].itertuples(index=False, name=None)),
    )

    conn.commit()
    cur.close()
    conn.close()
    print(f"Carregando dados... {len(df)} focos + {len(c)} registros de clima")


def analisar():
    hook = OracleHook(oracle_conn_id=ORACLE_CONN_ID)
    consultas = {
        "Focos por mes": (
            "SELECT mes, COUNT(*) total, SUM(ocorrencia_foco) focos "
            "FROM foco_queimada GROUP BY mes ORDER BY mes"
        ),
        "Distribuicao por risco": (
            "SELECT risco_classe, COUNT(*) qtd FROM foco_queimada "
            "GROUP BY risco_classe ORDER BY qtd DESC"
        ),
    }
    for titulo, sql in consultas.items():
        print(f"\n== {titulo} ==")
        for linha in hook.get_records(sql):
            print("  ", linha)


with DAG(
    dag_id="orbitalfire_bddi_pipeline",
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=["fiap", "global-solution", "espacial", "orbitalfire"],
) as dag:

    t_extrair_csv = PythonOperator(task_id="extrair_csv",        python_callable=extrair_csv)
    t_extrair_api = PythonOperator(task_id="extrair_clima_api",  python_callable=extrair_clima_api)
    t_transformar = PythonOperator(task_id="transformar",        python_callable=transformar)
    t_criar       = PythonOperator(task_id="criar_tabelas",      python_callable=criar_tabelas)
    t_carregar    = PythonOperator(task_id="carregar_oracle",    python_callable=carregar_oracle)
    t_analisar    = PythonOperator(task_id="analisar",           python_callable=analisar)

    [t_extrair_csv, t_extrair_api] >> t_transformar >> t_criar >> t_carregar >> t_analisar
