use sb_crm; 
set mapred.job.queue.name=crm_resource_pool;

insert overwrite table sb_crm.abts_dim_clientes select 
nvl(b.num_cliente,0) as cve_cliente
,nvl(a.num_cliente_padre,0) as cve_cliente_padre
,case 
when upper(nvl(a.personalidad,b.personalidad))='NULL' then 'X' 
when upper(nvl(a.personalidad,b.personalidad))='' then 'X'
when upper(nvl(a.personalidad,b.personalidad)) is null then 'X' 
else upper(nvl(a.personalidad,b.personalidad)) end as cve_tipo_persona
,nvl(nvl(a.nombre_hig,b.nom_cliente),'DESCONOCIDO') as nom_cliente_hig
,nvl(nvl(a.ap_materno_hig, b.nom_materno),'DESCONOCIDO') as nom_apellido_materno_hig
,nvl(nvl(a.ap_paterno_hig,b.nom_paterno),'DESCONOCIDO') as nom_apellido_paterno_hig
,nvl(nvl(a.fec_nacimiento,b.fec_nacimien),cast('9999-12-31 00:00:00.0' as timestamp)) as fyh_nacimiento_hig
,case 
when trim(upper(nvl(a.curp,b.curp)))='NULL' then 'DESCONOCIDO' 
when trim(upper(nvl(a.curp,b.curp)))='' then 'DESCONOCIDO'
when trim(upper(nvl(a.curp,b.curp))) is null then 'DESCONOCIDO' 
else trim(upper(nvl(a.curp,b.curp))) end as cve_curp_hig
,case 
when trim(upper(nvl(a.rfc,b.rfc_del_cliente)))='NULL' then 'DESCONOCIDO' 
when trim(upper(nvl(a.rfc,b.rfc_del_cliente)))='' then 'DESCONOCIDO'
when trim(upper(nvl(a.rfc,b.rfc_del_cliente))) is null then 'DESCONOCIDO' 
else trim(upper(nvl(a.rfc,b.rfc_del_cliente))) end as cve_rfc_hig
,nvl(TRIM(nvl(a.cod_sexo,b.cod_sexo)),'X') as cve_sexo_hig
,upper(nvl(a.cod_edo_civil,b.cod_estcivil)) as cve_estado_civil_hig
,nvl(TRIM(b.cod_regmatri),'X') as cve_regimen_matrimonial
,nvl(TRIM(b.cod_niv_estu),'000') as cve_nivel_estudio
,nvl(TRIM(b.cod_sit_pers),'000') as cve_situacion_persona
,nvl(TRIM(b.cod_cond_persona),'XXX') as cve_condicion_persona 
,nvl(TRIM(b.cod_act_econ),'00000000') as cve_activ_especifica
,nvl(TRIM(b.cod_sec_econ),'000') as cve_sector_economico 
,nvl(TRIM(b.tip_nat_econ),'000') as cve_activ_generica
,nvl(b.fec_alta,cast('9999-12-31 00:00:00.0' as timestamp)) as fyh_alta_cliente 
,${hiveconf:varCiclo} 
from 
	dm_macrobase_hist.v_cdd_maestra_demograficos_dl a right outer join 
	af_macrobase_hist.fac_ad_personas b on b.num_cliente=a.num_cliente
where 
	a.data_date_part=${hiveconf:varCiclo} and b.data_date_part='2018-07-20';


insert overwrite table sb_crm.abts_dim_geograficos select 
	 nvl(b.num_cliente,0) as cve_cliente
	,nvl(b.cod_pais_nac,'000') as cve_pais_nacimiento
	,nvl(b.cod_pais_res,'000') as cve_pais_residencia
	,nvl(b.cod_pais,'000') as cve_pais
	,nvl(a.cve_estado,0) as cod_entidad_federativa
	,nvl(a.estado,'XX') as cve_entidad_federativa
	,nvl(a.edo_homologado,'XX') as cve_entidad_fed_homo
	,nvl(nvl(a.nombre_estado,b.cod_estado),'DESCONOCIDO') as nom_entidad_federativa
	,nvl(a.cve_municipio,'00000') as cod_deleg_munic
	,nvl(nvl(a.localidad,b.cod_localida),'DESCONOCIDO') as nom_deleg_munic_hig
	,nvl(a.cve_ciudad,'00000') as cod_ciudad
	,nvl(nvl(a.ciudad,b.cod_ciudad),'DESCONOCIDO') as nom_ciudad_hig
	,nvl(nvl(a.colonia,b.des_colonia),'DESCONOCIDO') as nom_colonia_hig
	,nvl(nvl(a.direccion,b.nom_calle),'DESCONOCIDO') as nom_calle_hig
	,nvl(nvl(a.cod_postal,b.cod_postal),'00000') as cve_codigo_postal_hig
	,nvl(nvl(a.cod_sit_domi,b.cod_sit_domi),'X') as cve_situacion_domicilio
	,nvl(a.sec_domicil,0) as num_secuencia_domicilio
	,${hiveconf:varCiclo}
from 
	dm_macrobase_hist.v_cdd_maestra_demograficos_dl a right outer join 
	af_macrobase_hist.fac_ad_personas b on b.num_cliente=a.num_cliente 
where 
	a.data_date_part=${hiveconf:varCiclo} and b.data_date_part='2018-07-20';