insert overwrite table sb_crm.abts_dim_clientes select 
b.num_cliente as cve_cliente
,a.num_cliente_padre as cve_cliente_padre
,upper(nvl(a.personalidad,b.personalidad)) as cve_tipo_persona
,nvl(a.nombre_hig,b.nom_cliente) as nom_cliente_hig
,nvl(a.ap_materno_hig, b.nom_materno) as nom_apellido_materno_hig
,nvl(a.ap_paterno_hig,b.nom_paterno) as nom_apellido_paterno_hig
,nvl(a.fec_nacimiento,b.fec_nacimien) as fyh_nacimiento_hig
,upper(nvl(a.curp,b.curp)) as cve_curp_hig
,nvl(a.rfc,b.rfc_del_cliente) as cve_rfc_hig
,upper(nvl(a.cod_sexo,b.cod_sexo)) as cve_sexo_hig
,upper(nvl(a.cod_edo_civil,b.cod_estcivil)) as cve_estado_civil_hig
,b.cod_regmatri as cve_regimen_matrimonial
,b.cod_niv_estu as cve_nivel_estudio
,b.cod_sit_pers as cve_situacion_persona
,b.cod_cond_persona as cve_condicion_persona 
,b.cod_act_econ as cve_activ_especifica
,b.cod_sec_econ as cve_sector_economico 
,b.tip_nat_econ as cve_activ_generica
,b.fec_alta as fyh_alta_cliente 
,201807 
from 
dm_macrobase_hist.v_cdd_maestra_demograficos_dl a right outer join 
af_macrobase_hist.fac_ad_personas b on b.num_cliente=a.num_cliente
where 
a.data_date_part=201807 and b.data_date_part='2018-07-20';

SELECT
nvl(cve_cliente,0),
nvl(cve_cliente_padre,0),
nvl(cve_tipo_persona,'X'),
nvl(nom_cliente_hig,'DESCONOCIDO'),
nvl(nom_apellido_materno_hig,'DESCONOCIDO'),
nvl(nom_apellido_paterno_hig,'DESCONOCIDO'),
nvl(fyh_nacimiento_hig,'31/12/9999 00:00:00'),
NVL(TRIM(cve_curp_hig),'DESCONOCIDO'),
NVL(TRIM(cve_rfc_hig),'DESCONOCIDO'),
nvl(TRIM(cve_sexo_hig),'X'),
nvl(TRIM(cve_estado_civil_hig)),
nvl(TRIM(cve_regimen_matrimonial),'X'),
nvl(TRIM(cve_nivel_estudio),'000'),
nvl(TRIM(cve_situacion_persona),'000'),
nvl(TRIM(cve_condicion_persona),'XXX'),
nvl(TRIM(cve_activ_especifica),'00000000'),
nvl(TRIM(cve_sector_economico),'000'),
nvl(TRIM(cve_activ_generica),'000'),
nvl(fyh_alta_cliente,'31/12/9999 00:00:00')
FROM sb_crm.abts_dim_clientes;

select case when cve_tipo_persona is null then 'X' when cve_tipo_persona = 'NULL' then 'X' else cve_tipo_persona end as cve_tipo_persona 
FROM sb_crm.abts_dim_clientes limit 20;



