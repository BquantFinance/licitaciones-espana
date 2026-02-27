-- BORME schema: company registry data (separate from L0 — does not feed L1/L2)
CREATE SCHEMA IF NOT EXISTS borme;

CREATE TABLE IF NOT EXISTS borme.empresas (
    borme_id BIGSERIAL PRIMARY KEY,
    fecha_borme DATE,
    num_borme INTEGER,
    num_entrada INTEGER,
    empresa TEXT,
    empresa_norm TEXT,
    provincia TEXT,
    cod_provincia TEXT,
    tipo_borme TEXT,
    actos TEXT,
    domicilio TEXT,
    capital_euros NUMERIC(18,2),
    fecha_constitucion TEXT,
    objeto_social TEXT,
    hoja_registral TEXT,
    tomo TEXT,
    inscripcion TEXT,
    fecha_inscripcion TEXT,
    pdf_filename TEXT,
    UNIQUE (fecha_borme, num_entrada, empresa_norm)
);

CREATE TABLE IF NOT EXISTS borme.cargos (
    cargo_id BIGSERIAL PRIMARY KEY,
    fecha_borme DATE,
    num_entrada INTEGER,
    empresa TEXT,
    empresa_norm TEXT,
    provincia TEXT,
    hoja_registral TEXT,
    tipo_acto TEXT,
    cargo TEXT,
    persona TEXT,
    pdf_filename TEXT,
    UNIQUE (fecha_borme, num_entrada, cargo, persona, tipo_acto)
);

CREATE INDEX IF NOT EXISTS idx_borme_empresas_norm ON borme.empresas (empresa_norm);
CREATE INDEX IF NOT EXISTS idx_borme_empresas_fecha ON borme.empresas (fecha_borme);
CREATE INDEX IF NOT EXISTS idx_borme_cargos_empresa ON borme.cargos (empresa_norm);
CREATE INDEX IF NOT EXISTS idx_borme_cargos_persona ON borme.cargos (persona);
CREATE INDEX IF NOT EXISTS idx_borme_cargos_fecha ON borme.cargos (fecha_borme);
