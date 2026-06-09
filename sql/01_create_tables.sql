-- ===================================================================
-- OrbitalFire / BDDI - Modelagem das tabelas no Oracle (oracle.fiap.com.br/ORCL)
-- Executado automaticamente pela task criar_tabelas da DAG.
-- ===================================================================

-- Drop idempotente (ignora ORA-00942: tabela inexistente)
BEGIN EXECUTE IMMEDIATE 'DROP TABLE foco_queimada';
EXCEPTION WHEN OTHERS THEN IF SQLCODE != -942 THEN RAISE; END IF; END;
/
BEGIN EXECUTE IMMEDIATE 'DROP TABLE clima_atual';
EXCEPTION WHEN OTHERS THEN IF SQLCODE != -942 THEN RAISE; END IF; END;
/

-- Tabela fato: observacoes de risco de foco (fonte = CSV OrbitalFire)
CREATE TABLE foco_queimada (
    foco_id              NUMBER       PRIMARY KEY,
    mes                  NUMBER(2),
    temperatura_c        NUMBER(5,1),
    umidade_relativa     NUMBER(5,1),
    velocidade_vento_kmh NUMBER(5,1),
    precipitacao_mm      NUMBER(6,1),
    dias_sem_chuva       NUMBER(4),
    ndvi                 NUMBER(5,3),
    indice_fwi           NUMBER(6,1),
    latitude             NUMBER(8,4),
    longitude            NUMBER(8,4),
    altitude_m           NUMBER(6),
    tipo_cobertura       VARCHAR2(20),
    ocorrencia_foco      NUMBER(1),
    risco_classe         VARCHAR2(10),
    data_carga           DATE DEFAULT SYSDATE
);

-- Tabela de enriquecimento: clima atual por bioma (fonte = API Open-Meteo ao vivo)
CREATE TABLE clima_atual (
    clima_id         NUMBER       PRIMARY KEY,
    regiao           VARCHAR2(40),
    latitude         NUMBER(8,4),
    longitude        NUMBER(8,4),
    temperatura_c    NUMBER(5,1),
    umidade_relativa NUMBER(5,1),
    vento_kmh        NUMBER(5,1),
    precipitacao_mm  NUMBER(6,1),
    fonte            VARCHAR2(20),
    coletado_em      DATE DEFAULT SYSDATE
);
