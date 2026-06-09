-- ===================================================================
-- OrbitalFire / BDDI - Consultas analiticas (Oracle)
-- 7 consultas (minimo exigido: 5). Cobrem: filtros, agrupamentos,
-- funcoes de agregacao, ordenacao, funcao de janela e JOIN.
-- ===================================================================

-- -------------------------------------------------------------------
-- 1) Volume de registros e focos por mes  (analise temporal)
--    Conceitos: agrupamento, agregacao, ordenacao
-- -------------------------------------------------------------------
SELECT mes,
       COUNT(*)                              AS total_observacoes,
       SUM(ocorrencia_foco)                  AS total_focos,
       ROUND(AVG(ocorrencia_foco) * 100, 1)  AS taxa_foco_pct
FROM   foco_queimada
GROUP  BY mes
ORDER  BY mes;

-- -------------------------------------------------------------------
-- 2) Estatisticas climaticas por tipo de cobertura do solo
--    Conceitos: agrupamento, MIN/MAX/AVG, ordenacao
-- -------------------------------------------------------------------
SELECT tipo_cobertura,
       COUNT(*)                          AS registros,
       ROUND(AVG(temperatura_c), 1)      AS temp_media,
       ROUND(MIN(umidade_relativa), 1)   AS umid_min,
       ROUND(MAX(indice_fwi), 1)         AS fwi_max,
       ROUND(AVG(ocorrencia_foco)*100,1) AS taxa_foco_pct
FROM   foco_queimada
GROUP  BY tipo_cobertura
ORDER  BY taxa_foco_pct DESC;

-- -------------------------------------------------------------------
-- 3) Comparacao estacao seca (mai-set) x estacao umida
--    Conceitos: filtro com CASE, agrupamento, agregacao
-- -------------------------------------------------------------------
SELECT CASE WHEN mes BETWEEN 5 AND 9 THEN 'Estacao seca'
            ELSE 'Estacao umida' END        AS estacao,
       COUNT(*)                             AS observacoes,
       ROUND(AVG(temperatura_c), 1)         AS temp_media,
       ROUND(AVG(dias_sem_chuva), 1)        AS dias_secos_media,
       ROUND(AVG(ocorrencia_foco) * 100, 1) AS taxa_foco_pct
FROM   foco_queimada
GROUP  BY CASE WHEN mes BETWEEN 5 AND 9 THEN 'Estacao seca'
               ELSE 'Estacao umida' END
ORDER  BY taxa_foco_pct DESC;

-- -------------------------------------------------------------------
-- 4) Ranking dos 5 meses com mais focos  (ranking)
--    Conceitos: agregacao, ordenacao, limite de linhas (ROWNUM)
-- -------------------------------------------------------------------
SELECT *
FROM (
    SELECT mes,
           SUM(ocorrencia_foco) AS total_focos
    FROM   foco_queimada
    GROUP  BY mes
    ORDER  BY total_focos DESC
)
WHERE ROWNUM <= 5;

-- -------------------------------------------------------------------
-- 5) Distribuicao por classe de risco com % do total
--    Conceitos: funcao de janela (SUM OVER), agregacao
-- -------------------------------------------------------------------
SELECT risco_classe,
       COUNT(*)                                            AS registros,
       ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 1)  AS percentual,
       ROUND(AVG(indice_fwi), 1)                           AS fwi_medio
FROM   foco_queimada
GROUP  BY risco_classe
ORDER  BY registros DESC;

-- -------------------------------------------------------------------
-- 6) Focos de alto risco em condicao critica  (filtros compostos)
--    Conceitos: filtro (WHERE com multiplas condicoes), ordenacao
-- -------------------------------------------------------------------
SELECT foco_id, tipo_cobertura, temperatura_c, umidade_relativa,
       dias_sem_chuva, indice_fwi, risco_classe
FROM   foco_queimada
WHERE  risco_classe = 'critico'
  AND  umidade_relativa < 30
  AND  dias_sem_chuva   > 20
ORDER  BY indice_fwi DESC
FETCH FIRST 10 ROWS ONLY;   -- Oracle 12c+; em 11g trocar por subquery + ROWNUM

-- -------------------------------------------------------------------
-- 7) Cruzamento dos focos com o clima atual coletado por API (JOIN)
--    Conceitos: JOIN espacial aproximado (mesma faixa de latitude),
--    agrupamento e agregacao
-- -------------------------------------------------------------------
SELECT c.regiao,
       c.temperatura_c                        AS temp_atual_api,
       COUNT(f.foco_id)                       AS focos_na_faixa,
       ROUND(AVG(f.indice_fwi), 1)            AS fwi_medio_historico
FROM   clima_atual c
JOIN   foco_queimada f
       ON ROUND(f.latitude) = ROUND(c.latitude)
GROUP  BY c.regiao, c.temperatura_c
ORDER  BY focos_na_faixa DESC;
