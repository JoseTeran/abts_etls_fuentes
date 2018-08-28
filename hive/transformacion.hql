insert overwrite table sb_crm.abts_dim_clientes select 
nvl(b.num_cliente,0) as cve_cliente
,nvl(a.num_cliente_padre,0) as cve_cliente_padre
,case 
when upper(nvl(a.personalidad,b.personalidad))='NULL' then 'X' 
when upper(nvl(a.personalidad,b.personalidad))='' then 'X'
else UPPER(nvl(a.personalidad,b.personalidad)) end as cve_tipo_persona
,nvl(nvl(a.nombre_hig,b.nom_cliente),'DESCONOCIDO') as nom_cliente_hig
,nvl(nvl(a.ap_materno_hig, b.nom_materno),'DESCONOCIDO') as nom_apellido_materno_hig
,nvl(nvl(a.ap_paterno_hig,b.nom_paterno),'DESCONOCIDO') as nom_apellido_paterno_hig
,nvl(nvl(a.fec_nacimiento,b.fec_nacimien),'31/12/9999 00:00:00') as fyh_nacimiento_hig
,nvl(a.curp,b.curp) as cve_curp_hig
,nvl(a.rfc,b.rfc_del_cliente) as cve_rfc_hig
,nvl(TRIM(nvl(a.cod_sexo,b.cod_sexo)),'X') as cve_sexo_hig
,nvl(a.cod_edo_civil,b.cod_estcivil) as cve_estado_civil_hig
,nvl(TRIM(b.cod_regmatri),'X') as cve_regimen_matrimonial
,nvl(TRIM(b.cod_niv_estu),'000') as cve_nivel_estudio
,nvl(TRIM(b.cod_sit_pers),'000') as cve_situacion_persona
,nvl(TRIM(b.cod_cond_persona),'XXX') as cve_condicion_persona 
,nvl(TRIM(b.cod_act_econ),'00000000') as cve_activ_especifica
,nvl(TRIM(b.cod_sec_econ),'000') as cve_sector_economico 
,nvl(TRIM(b.tip_nat_econ),'000') as cve_activ_generica
,nvl(b.fec_alta,'31/12/9999 00:00:00') as fyh_alta_cliente 
,201807 
from 
dm_macrobase_hist.v_cdd_maestra_demograficos_dl a right outer join 
af_macrobase_hist.fac_ad_personas b on b.num_cliente=a.num_cliente
where 
a.data_date_part=201807 and b.data_date_part='2018-07-20';



insert overwrite table sb_crm.abts_dim_clientes select 
    nvl(b.num_cliente,0) as cve_cliente
    ,nvl(a.num_cliente_padre,0) as cve_cliente_padre
    ,if(upper(nvl(nvl(a.personalidad,b.personalidad),'X'))='NULL','X',upper(nvl(nvl(a.personalidad,b.personalidad),'X'))) as cve_tipo_persona
    ,nvl(nvl(a.nombre_hig,b.nom_cliente),'DESCONOCIDO') as nom_cliente_hig
    ,nvl(nvl(a.ap_materno_hig, b.nom_materno),'DESCONOCIDO') as nom_apellido_materno_hig
    ,nvl(nvl(a.ap_paterno_hig,b.nom_paterno),'DESCONOCIDO') as nom_apellido_paterno_hig
    ,nvl(nvl(a.fec_nacimiento,b.fec_nacimien),'31/12/9999 00:00:00') as fyh_nacimiento_hig
    ,decode(NVL(TRIM(UPPER(nvl(a.curp,b.curp))),'DESCONOCIDO'),"null",'DESCONOCIDO',TRIM(UPPER(cve_rfc_hig))) as cve_curp_hig
    ,nvl(a.rfc,b.rfc_del_cliente) as cve_rfc_hig
    ,nvl(nvl(a.cod_sexo,b.cod_sexo),'X') as cve_sexo_hig
    ,nvl(nvl(a.cod_edo_civil,b.cod_estcivil ),'X') as cve_estado_civil_hig
    ,nvl(b.cod_regmatri,'X') as cve_regimen_matrimonial
    ,nvl(b.cod_niv_estu,'000') as cve_nivel_estudio
    ,nvl(b.cod_sit_pers,'000') as cve_situacion_persona
    ,nvl(b.cod_cond_persona,'XXX') as cve_condicion_persona 
    ,nvl(b.cod_act_econ,'00000000') as cve_activ_especifica
    ,nvl(b.cod_sec_econ,'000') as cve_sector_economico 
    ,nvl(b.tip_nat_econ,'000') as cve_activ_generica
    ,nvl(b.fec_alta,'31/12/9999 00:00:00') as fyh_alta_cliente 
    ,201807 
from 
    dm_macrobase_hist.v_cdd_maestra_demograficos_dl a right outer join 
    af_macrobase_hist.fac_ad_personas b on b.num_cliente=a.num_cliente
where 
    a.data_date_part=201807 and b.data_date_part='2018-07-20';

    DECODE(NVL(TRIM(UPPER(cve_curp_hig)),'DESCONOCIDO'),'NULL','DESCONOCIDO',TRIM(UPPER(cve_rfc_hig))), --OJO


decode(NVL(TRIM(UPPER(nvl(a.curp,b.curp))),'DESCONOCIDO'),'NULL','DESCONOCIDO',TRIM(UPPER(cve_rfc_hig)))


LOAD DATA LOCAL INPATH '/home/n557453/datos.csv' OVERWRITE INTO TABLE sb_crm.mytable;

replace(upper(nvl(cve_tipo_persona,'X') ),'NULL','X'), --OJO

select case 
when upper(nvl(a.personalidad,b.personalidad))='NULL' then 'X' 
when upper(nvl(a.personalidad,b.personalidad))='' then 'X'
when nvl(a.personalidad,b.personalidad) is null then 'X' 
else UPPER(nvl(a.personalidad,b.personalidad)) end 
from sb_crm.abts_dim_clientes limit 20;

select  from sb_crm.abts_dim_clientes where cve_curp_hig is null limit 10;



select nvl(a.personalidad,b.personalidad) as cve_tipo_persona
from dm_macrobase_hist.v_cdd_maestra_demograficos_dl a right outer join 
af_macrobase_hist.fac_ad_personas b on b.num_cliente=a.num_cliente
where 
a.data_date_part=201807 and a.personalidad is null and b.data_date_part='2018-07-20' limit 20;