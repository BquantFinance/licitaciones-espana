# `ted.zip`

Esquema de los Parquet de `ted.zip` en la release v2026.10, leído de sus pies con [`scripts/esquema_release.py`](../../scripts/esquema_release.py) (no a mano). Qué es cada tabla y cómo usarla: [ficha de la fuente](../fuentes/ted.md). Las marcas `_…` comunes: [`docs/ESQUEMA.md`](../ESQUEMA.md).

| Fichero | Filas | Columnas |
|---|--:|--:|
| `ted_es_can.parquet` | 857.017 | 164 |

## `ted_es_can.parquet`

857.017 filas · 164 columnas · 1 grupo de filas

| # | Columna | Tipo | |
|--:|---|---|---|
| 1 | `ted_notice_id` | `large_string` |  |
| 2 | `TED_NOTICE_URL` | `large_string` |  |
| 3 | `year` | `int64` |  |
| 4 | `ID_TYPE` | `large_string` |  |
| 5 | `dt_dispatch` | `timestamp[us]` |  |
| 6 | `XSD_VERSION` | `large_string` |  |
| 7 | `cancelled` | `large_string` |  |
| 8 | `CORRECTIONS` | `large_string` |  |
| 9 | `B_MULTIPLE_CAE` | `large_string` |  |
| 10 | `cae_name` | `large_string` |  |
| 11 | `cae_nationalid` | `large_string` |  |
| 12 | `CAE_ADDRESS` | `large_string` |  |
| 13 | `cae_town` | `large_string` |  |
| 14 | `CAE_POSTAL_CODE` | `large_string` |  |
| 15 | `CAE_GPA_ANNEX` | `large_string` |  |
| 16 | `iso_country` | `large_string` |  |
| 17 | `ISO_COUNTRY_CODE_GPA` | `large_string` |  |
| 18 | `B_MULTIPLE_COUNTRY` | `large_string` |  |
| 19 | `ISO_COUNTRY_CODE_ALL` | `large_string` |  |
| 20 | `cae_type` | `large_string` |  |
| 21 | `EU_INST_CODE` | `large_string` |  |
| 22 | `MAIN_ACTIVITY` | `large_string` |  |
| 23 | `B_ON_BEHALF` | `large_string` |  |
| 24 | `B_INVOLVES_JOINT_PROCUREMENT` | `large_string` |  |
| 25 | `B_AWARDED_BY_CENTRAL_BODY` | `large_string` |  |
| 26 | `type_of_contract` | `large_string` |  |
| 27 | `nuts` | `large_string` |  |
| 28 | `is_framework` | `large_string` |  |
| 29 | `FRA_ESTIMATED` | `large_string` |  |
| 30 | `B_FRA_CONTRACT` | `large_string` |  |
| 31 | `B_DYN_PURCH_SYST` | `large_string` |  |
| 32 | `cpv` | `large_string` |  |
| 33 | `MAIN_CPV_CODE_GPA` | `large_string` |  |
| 34 | `ID_LOT` | `large_string` |  |
| 35 | `ADDITIONAL_CPVS` | `large_string` |  |
| 36 | `B_GPA` | `large_string` |  |
| 37 | `GPA_COVERAGE` | `large_string` |  |
| 38 | `lots_number` | `large_string` |  |
| 39 | `VALUE_EURO` | `large_string` |  |
| 40 | `value_euro` | `double` |  |
| 41 | `VALUE_EURO_FIN_2` | `large_string` |  |
| 42 | `B_EU_FUNDS` | `large_string` |  |
| 43 | `top_type` | `large_string` |  |
| 44 | `B_ACCELERATED` | `large_string` |  |
| 45 | `OUT_OF_DIRECTIVES` | `large_string` |  |
| 46 | `CRIT_CODE` | `large_string` |  |
| 47 | `CRIT_PRICE_WEIGHT` | `large_string` |  |
| 48 | `CRIT_CRITERIA` | `large_string` |  |
| 49 | `CRIT_WEIGHTS` | `large_string` |  |
| 50 | `B_ELECTRONIC_AUCTION` | `large_string` |  |
| 51 | `number_awards` | `large_string` |  |
| 52 | `ted_award_id` | `large_string` |  |
| 53 | `lot_id` | `large_string` |  |
| 54 | `INFO_ON_NON_AWARD` | `large_string` |  |
| 55 | `INFO_UNPUBLISHED` | `large_string` |  |
| 56 | `B_AWARDED_TO_A_GROUP` | `large_string` |  |
| 57 | `win_name` | `large_string` |  |
| 58 | `win_nationalid` | `large_string` |  |
| 59 | `WIN_ADDRESS` | `large_string` |  |
| 60 | `WIN_TOWN` | `large_string` |  |
| 61 | `WIN_POSTAL_CODE` | `large_string` |  |
| 62 | `win_country` | `large_string` |  |
| 63 | `B_CONTRACTOR_SME` | `large_string` |  |
| 64 | `CONTRACT_NUMBER` | `large_string` |  |
| 65 | `TITLE` | `large_string` |  |
| 66 | `number_offers` | `double` |  |
| 67 | `NUMBER_TENDERS_SME` | `large_string` |  |
| 68 | `NUMBER_TENDERS_OTHER_EU` | `large_string` |  |
| 69 | `NUMBER_TENDERS_NON_EU` | `large_string` |  |
| 70 | `NUMBER_OFFERS_ELECTR` | `large_string` |  |
| 71 | `AWARD_EST_VALUE_EURO` | `large_string` |  |
| 72 | `AWARD_VALUE_EURO` | `large_string` |  |
| 73 | `award_value_euro` | `double` |  |
| 74 | `B_SUBCONTRACTED` | `large_string` |  |
| 75 | `dt_award` | `timestamp[us]` |  |
| 76 | `source` | `large_string` |  |
| 77 | `notice_type` | `large_string` |  |
| 78 | `notice_subtype` | `large_string` |  |
| 79 | `form_type` | `large_string` |  |
| 80 | `publication_date` | `timestamp[us]` |  |
| 81 | `procedure_type` | `large_string` |  |
| 82 | `contract_nature` | `large_string` |  |
| 83 | `place_of_performance` | `large_string` |  |
| 84 | `title_proc` | `large_string` |  |
| 85 | `description_proc` | `large_string` |  |
| 86 | `buyer_legal_type` | `large_string` |  |
| 87 | `buyer_contracting_entity` | `large_string` |  |
| 88 | `buyer_profile` | `large_string` |  |
| 89 | `total_value` | `double` |  |
| 90 | `total_value_cur` | `large_string` |  |
| 91 | `estimated_value_proc` | `double` |  |
| 92 | `estimated_value_proc_cur` | `large_string` |  |
| 93 | `procedure_id` | `large_string` |  |
| 94 | `internal_id_proc` | `large_string` |  |
| 95 | `modification_prev_notice` | `large_string` |  |
| 96 | `direct_award_justification` | `large_string` |  |
| 97 | `direct_award_justification_text` | `large_string` |  |
| 98 | `sme_participation` | `large_string` |  |
| 99 | `notice_identifier` | `large_string` |  |
| 100 | `notice_version` | `large_string` |  |
| 101 | `changed_notice` | `large_string` |  |
| 102 | `change_reason_code` | `large_string` |  |
| 103 | `notice_value` | `double` |  |
| 104 | `notice_value_cur` | `large_string` |  |
| 105 | `notice_framework_max_value` | `double` |  |
| 106 | `notice_framework_max_value_cur` | `large_string` |  |
| 107 | `notice_framework_approx_value` | `double` |  |
| 108 | `notice_framework_approx_value_cur` | `large_string` |  |
| 109 | `lot_index` | `double` |  |
| 110 | `n_filas_aviso` | `double` |  |
| 111 | `lot_result_id` | `large_string` |  |
| 112 | `internal_id_lot` | `large_string` |  |
| 113 | `title_lot` | `large_string` |  |
| 114 | `description_lot` | `large_string` |  |
| 115 | `cpv_lot` | `large_string` |  |
| 116 | `estimated_value_lot` | `double` |  |
| 117 | `estimated_value_lot_cur` | `large_string` |  |
| 118 | `duration_lot` | `double` |  |
| 119 | `duration_lot_unit` | `large_string` |  |
| 120 | `award_criterion_type` | `large_string` |  |
| 121 | `award_criterion_weight` | `large_string` |  |
| 122 | `winner_selection_status` | `large_string` |  |
| 123 | `non_award_justification` | `large_string` |  |
| 124 | `n_ofertas_descritas` | `large_string` |  |
| 125 | `tender_value_lowest` | `double` |  |
| 126 | `tender_value_lowest_cur` | `large_string` |  |
| 127 | `tender_value_highest` | `double` |  |
| 128 | `tender_value_highest_cur` | `large_string` |  |
| 129 | `framework_max_lot` | `double` |  |
| 130 | `framework_max_lot_cur` | `large_string` |  |
| 131 | `framework_est_value` | `double` |  |
| 132 | `framework_est_value_cur` | `large_string` |  |
| 133 | `tender_id` | `large_string` |  |
| 134 | `ganadora_por` | `large_string` | [marca](../ESQUEMA.md#ted) |
| 135 | `tender_reference` | `large_string` |  |
| 136 | `tender_rank` | `large_string` |  |
| 137 | `tender_value` | `double` |  |
| 138 | `tender_value_cur` | `large_string` |  |
| 139 | `subcontracting_value` | `double` |  |
| 140 | `subcontracting_value_cur` | `large_string` |  |
| 141 | `paid_amount` | `double` |  |
| 142 | `paid_amount_cur` | `large_string` |  |
| 143 | `penalties_amount` | `double` |  |
| 144 | `penalties_amount_cur` | `large_string` |  |
| 145 | `tendering_party_name` | `large_string` |  |
| 146 | `win_platform_id` | `large_string` |  |
| 147 | `win_town` | `large_string` |  |
| 148 | `win_size` | `large_string` |  |
| 149 | `contract_id` | `large_string` |  |
| 150 | `contract_award_dates` | `large_string` |  |
| 151 | `contract_conclusion_date` | `large_string` |  |
| 152 | `contract_title` | `large_string` |  |
| 153 | `contract_framework` | `large_string` |  |
| 154 | `_xml_eforms` | `large_string` | [marca](../ESQUEMA.md#ted) |
| 155 | `_origen` | `string` | [marca](../ESQUEMA.md#control-de-versiones) |
| 156 | `currency` | `large_string` |  |
| 157 | `importe_ted` | `double` |  |
| 158 | `win_nif_clean` | `large_string` |  |
| 159 | `cae_nif_clean` | `large_string` |  |
| 160 | `cpv_2` | `large_string` |  |
| 161 | `tipo_contrato` | `large_string` |  |
| 162 | `_primera_descarga` | `large_string` | [marca](../ESQUEMA.md#control-de-versiones) |
| 163 | `_ultima_descarga` | `large_string` | [marca](../ESQUEMA.md#control-de-versiones) |
| 164 | `_en_ultima_descarga` | `bool` | [marca](../ESQUEMA.md#control-de-versiones) |

## Carpeta `v2026.02/`

Ficheros de v2026.02 que van tal cual en esta release (7, 7 Parquet), porque la descarga actual no los cubre: su esquema es el de v2026.02. La columna `nota` del `MANIFEST.csv` del ZIP dice por qué está cada uno.

| Fichero | Filas | Columnas |
|---|--:|--:|
| `v2026.02/crossval_matched.parquet` | 597.061 | 39 |
| `v2026.02/crossval_missing.parquet` | 728.670 | 25 |
| `v2026.02/crossval_sara.parquet` | 1.341.022 | 39 |
| `v2026.02/crossval_sara_v2.parquet` | 1.287.327 | 38 |
| `v2026.02/missing_alta_confianza.parquet` | 189.028 | 25 |
| `v2026.02/missing_alta_confianza_v2.parquet` | 244.384 | 19 |
| `v2026.02/missing_final.parquet` | 908.896 | 20 |

---

[Todos los ZIP](README.md) · [Marcas comunes](../ESQUEMA.md)
