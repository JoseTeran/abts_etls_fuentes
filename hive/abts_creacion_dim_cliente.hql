--INDICAR EL ESQUEMA A UTILIZAR
USE SB_CRM;

--ESPECIFICAR LA PILA 
SET MAPRED.JOB.QUEUE.NAME=CRM_RESOURCE_POOL;


--Dimension abts_dim_clientes
create table sb_crm.abts_dim_clientes (
cve_cliente             bigint
,cve_cliente_padre      bigint
,cve_tipo_persona        string
,nom_cliente_hig         string
,nom_apellido_materno_hig        string
,nom_apellido_paterno_hig        string
,fyh_nacimiento_hig      timestamp
,cve_curp_hig            string
,cve_rfc_hig             string
,cve_sexo_hig            string
,cve_estado_civil_hig    string
,cve_regimen_matrimonial string
,cve_nivel_estudio       string
,cve_situacion_persona   string
,cve_condicion_persona   string
,cve_activ_especifica    string
,cve_sector_economico    string
,cve_activ_generica      string
,fyh_alta_cliente        timestamp
,num_anio_mes            int
) stored as parquet;

--Dimension abts_dim_geograficos
create table sb_crm.abts_dim_geograficos  (
cve_cliente             bigint
,cve_pais_nacimiento     string
,cve_pais_residencia     string
,cve_pais                string
,cod_entidad_federativa  string
,cve_entidad_federativa  string
,cve_entidad_fed_homo    string
,nom_entidad_federativa  string
,cod_deleg_munic         string
,nom_deleg_munic_hig     string
,cod_ciudad              string
,nom_ciudad_hig          string
,nom_colonia_hig         string
,nom_calle_hig           string
,cve_codigo_postal_hig   string
,cve_situacion_domicilio string
,num_secuencia_domicilio int
,num_anio_mes            int
) stored as parquet;
