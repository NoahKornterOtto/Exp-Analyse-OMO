--Ca 1GB
SELECT 
  MARKE, 
  ARTIKELNUMMER, 
  ITEM_H_SK, 
  SEASON, 
  MKZ,
  KATEGORIE_BEZEICHNUNG, 
  NOS, 
  VEK, 
  BEK, 
  STAMM_VK
FROM 
  `{dataset_id}.{table_id}`
WHERE 
  season = 149 AND OT = 4 AND FT = 69

