select top 20 * FROM Nvdev.wbr_analyst_dim
where rollup_site like '%SJO%'
and country = 'USA'
