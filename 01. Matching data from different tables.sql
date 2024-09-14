DROP TABLE IF EXISTS #AB

-- Create temporary table #AB with data from QuadroA and QuadroB
SELECT *
INTO #AB
FROM [ABC].[XYZ].[QuadroA]
WHERE Periodo >= 202201
  AND PrzContratual <> 0
  AND OprRenegociada IN (0, 1)

UNION ALL

SELECT *
FROM [ABC].[XYZ].[QuadroB]
WHERE Periodo >= 202201
  AND PrzContratual <> 0
  AND OprRenegociada IN (0, 1)


DROP TABLE IF EXISTS #AC

-- Create temporary table #AC

SELECT 
    a.[SK_ContInst],
    a.[CodEntidade],
    a.[Periodo],
    CASE 
        WHEN a.CodEntidade = '1234' AND a.Periodo = 202208 AND a.OprRenegociada = 0
        THEN b.montIni_alocado / 1000000
        ELSE a.Montante
    END AS Montante,
    a.[TAA],
    a.[OprRenegociada],
    a.[Finalidade],
    b.[TIN_CodCRC],
    a.[SetorInst_Mt]
INTO #AC
FROM #AB a
LEFT JOIN (
    SELECT DISTINCT 
        CntInst_SK_ContInst, 
        LEFT(dtref, 6) AS Periodo, 
        TIN_CodCRC, 
        TAA, 
        montIni_alocado 
    FROM [XPTO].[UIP_MOD01].[TBPVICRD0_CRC_STAT] 
    WHERE dtref >= 20220100
) b
ON a.Periodo = b.Periodo 
AND a.SK_ContInst = b.CntInst_SK_ContInst
WHERE a.Finalidade IN ('20', '01') 
  AND a.Periodo >= 202201 
  AND a.TAA IS NOT NULL 
  AND a.Montante > 0

-- Final result selection 

SELECT
    Periodo,
    CodEntidade,
    TIN_CodCRC,
    SetorInst_Mt,
    Finalidade,
    OprRenegociada,
    SUM(Montante) AS Montante,
    SUM(Montante * TAA) AS TAA_aux
FROM #AC
GROUP BY 
    Periodo,
    CodEntidade,
    TIN_CodCRC,
    SetorInst_Mt,
    Finalidade,
    OprRenegociada
ORDER BY Periodo, CodEntidade ASC
