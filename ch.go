package df

/*


CREATE TABLE econGo.final
(
    `zip` String COMMENT 'zip code',
    `cbsa` String COMMENT 'core-based statistical area (non-division)',
    `cbsaDiv` String COMMENT 'core-based statistical area (with divisions)',
    `county` String COMMENT 'county code',
    `zip3` String COMMENT '3-digit zip',
    `msaDiv` String COMMENT 'msa code (with divisions)',
    `state` String COMMENT 'state',
    `month` Date COMMENT 'as-of date for the data',
    `city` String COMMENT 'city',
    `msaName` String COMMENT 'msa name',
    `countyName` String COMMENT 'county name',
    `msaDHpi` Float64 COMMENT 'fhfa hpi for msa (with divisions): 0 if not in msa',
    `stateHpi` Float64 COMMENT 'fhfa hpi for the state + PR',
    `nonMetroHpi` Float64 COMMENT 'fhfa hpi for non-metro areas in the state',
    `zip3Hpi` Float64 COMMENT 'fhfa hpi at 3-digit zip level',
    `unempRate` Float64 COMMENT 'bls county-level unemployment rate (not seasonally adjusted)',
    `lbrForce` Float64 COMMENT 'bls county-level labor force (not seasonally adjusted)',
    `cepi` Float32 COMMENT 'consumer expenditure price index, PCEPI',
    `treas10` Float64 COMMENT '10 year treasury, fred DGS10',
    `mortFix30` Float64 COMMENT '30-year fixed mortgage rate, fred MORTGAGE30US',
    `mortFix15` Float64 COMMENT '15-year fixed mortgage rate, fred MORTGAGE15US',
    `mortArm5` Float64 COMMENT '5/1 ARM teaser rate, fred MORTGAGE5US',
    `stateValue` String COMMENT 'Y/N/X, Y = state average used, X=missing',
    `q10` Float64 COMMENT '10th %ile of income, lognormal fit',
    `q25` Float64 COMMENT '25th %ile of income, lognormal fit',
    `q50` Float64 COMMENT 'median income, lognormal fit',
    `q75` Float64 COMMENT '75th %ile of income, lognormal fit',
    `q90` Float64 COMMENT '90th %ile of income, lognormal fit',
    `n` Int64 COMMENT '# of returns'
)
ENGINE = MergeTree
ORDER BY (zip, month)
SETTINGS index_granularity = 8192
*/
