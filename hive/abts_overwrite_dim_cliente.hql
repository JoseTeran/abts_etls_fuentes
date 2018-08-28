use sb_crm; 
set mapred.job.queue.name=crm_resource_pool;

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
	,${hiveconf:varCiclo} 
from 
	dm_macrobase_hist.v_cdd_maestra_demograficos_dl a right outer join 
	af_macrobase_hist.fac_ad_personas b on b.num_cliente=a.num_cliente
where 
	a.data_date_part=${hiveconf:varCiclo} and b.data_date_part='2018-07-20';

--dim_geograficos
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