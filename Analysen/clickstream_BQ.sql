SELECT
        d.ot_vid_tok AS ot_vid
        , d.ot_SessionId_tok
        , ot_PageCluster
        --, f.product_pd_VariationId AS variationid -- needed to join with a4l
        , f.product_pd_ArticleNumber AS articleNumber
        , MIN(d.ot_utc) AS timestamp_order -- needed to join with a4l
        , SUM(CAST(f.feature_order_Quantity AS FLOAT64)) AS units
    FROM `brain-clickstream-prd.datalake_v1_ov_lhotse_clickstream_optin.datadynamic` d
        LEFT JOIN `brain-clickstream-prd.datalake_v1_ov_lhotse_clickstream_optin.datadynamicfeatures` f
        ON d.dl_brain_message_hash = f.dl_brain_message_hash
        AND DATE(f.partition_day_utc, "Europe/Berlin") >= "2024-02-26" AND DATE(f.partition_day_utc, "Europe/Berlin") < "2024-02-27" -- add one week -- handle NULL values using as an ON condition
    WHERE
        DATE(d.partition_day_utc, "Europe/Berlin") >= "2024-02-26" AND DATE(d.partition_day_utc, "Europe/Berlin") < "2024-02-27" -- add one week
        -- bot filter
        AND ot_bot IS NULL
        AND ot_InternalTraffic IS NULL
        AND (d.user_testaccount IS NULL OR d.user_testaccount LIKE 'false' OR d.user_testaccount LIKE 'NULL')
        --AND ot_Agent NOT IN (SELECT agent FROM botfilter.user_agents WHERE day >= '2023-01-01' AND day < '2023-01-02')
        -- filtering on orders (do not filter on any wk-labels!)
        AND d.ot_PageCluster = "Suchergebnisseite"
        --AND f.dl_feature_name = "Variation"
        AND f.product_pd_VariationId IS NOT NULL -- clean up missing data
      --  AND f.feature_order_Quantity NOT LIKE "%|%" -- clean up merging errors
    GROUP BY 1,2,3,4