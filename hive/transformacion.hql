set mapred.job.queue.name=crm_resource_pool;

/*Transformacion de atributo personalidad*/
select  
case 
when upper(nvl(a.personalidad,b.personalidad))='NULL' then 'X' 
when upper(nvl(a.personalidad,b.personalidad))='' then 'X'
when upper(nvl(a.personalidad,b.personalidad)) is null then 'X' 
else upper(nvl(a.personalidad,b.personalidad)) end as cve_tipo_persona
from 
dm_macrobase_hist.v_cdd_maestra_demograficos_dl a right outer join 
af_macrobase_hist.fac_ad_personas b on b.num_cliente=a.num_cliente
where 
a.data_date_part=201807 and b.data_date_part='2018-07-20' limit 20;

/*Transformacion de curp*/
select  
case 
when trim(upper(nvl(a.curp,b.curp)))='NULL' then 'DESCONOCIDO' 
when trim(upper(nvl(a.curp,b.curp)))='' then 'DESCONOCIDO'
when trim(upper(nvl(a.curp,b.curp))) is null then 'DESCONOCIDO' 
else trim(upper(nvl(a.curp,b.curp))) end as cve_curp_hig
from 
dm_macrobase_hist.v_cdd_maestra_demograficos_dl a right outer join 
af_macrobase_hist.fac_ad_personas b on b.num_cliente=a.num_cliente
where 
a.data_date_part=201807 and b.data_date_part='2018-07-20' limit 20;

/*Transformacion de rfc*/
select  
case 
when trim(upper(nvl(a.rfc,b.rfc_del_cliente)))='NULL' then 'DESCONOCIDO' 
when trim(upper(nvl(a.rfc,b.rfc_del_cliente)))='' then 'DESCONOCIDO'
when trim(upper(nvl(a.rfc,b.rfc_del_cliente))) is null then 'DESCONOCIDO' 
else trim(upper(nvl(a.rfc,b.rfc_del_cliente))) end as cve_rfc_hig
from 
dm_macrobase_hist.v_cdd_maestra_demograficos_dl a right outer join 
af_macrobase_hist.fac_ad_personas b on b.num_cliente=a.num_cliente
where 
a.data_date_part=201807 and b.data_date_part='2018-07-20' limit 20;

select distinct cve_tipo_persona from sb_crm.abts_dim_clientes;
select distinct cve_curp_hig from sb_crm.abts_dim_clientes;
select distinct cve_rfc_hig from sb_crm.abts_dim_clientes;
