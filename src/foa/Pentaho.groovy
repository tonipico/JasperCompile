package foa

import groovy.sql.Sql

class Pentaho {
	
	static execute(map) {
		
		def sql = Sql.newInstance(
			'jdbc:postgresql://erpnext-bi.foa.com.py:5432/erpnext_bi',
			'postgres',
			'*v6a9rA3%j',
			'org.postgresql.Driver'
		)
		
		sql.execute "drop table if exists t_gross_profit"
		
		def query = """
create table t_gross_profit as
with t_si as (
    select item.parenttype, item.parent, item.name "item_row",
           si.posting_date,
           si.posting_ts,
           si.project,
           si.update_stock,
           si.customer,
           si.customer_group,
           si.territory,
           item.item_code,
           item.warehouse,
           item.dn_detail,
           item.delivery_note,
           item.qty,
           item.base_net_rate,
           item.base_net_amount,
           item.cost_center,
           ti.business_center,
           ti.is_stock_item,
           row_number() over(order by posting_ts desc, item.name) rownum
    from t_sales_invoice si
    join t_sales_invoice_item item on item.parent = si.name
    join t_item ti on ti.name = item.item_code
    where 1=1
      and si.company = ${map.company}
      and (${map.include_returned} = 'Si' or is_return = 0)
      and posting_date >= to_date(${map.from_date}, 'yyyy-mm-dd')
      and posting_date <= to_date(${map.to_date}, 'yyyy-mm-dd')
    order by posting_ts desc, item_row
)

-- *** Busca por code y ware por fecha
, g_item_code_warehouse as (
    select item_code, warehouse, max(posting_ts) max_posting_ts, min(rownum) min_rownum
    from t_si
    where dn_detail is null
    group by item_code, warehouse
)
, s_buy_amount_code_ware as (
    select t2.item_code, t2.warehouse, min_rownum,
           last_value(t2.valuation_rate) over (partition by t2.item_code
             order by t2.min_rownum desc rows between unbounded preceding and unbounded following) valuation_rate
    from (
        select distinct t.item_code, t.warehouse, min_rownum,
                last_value(sle.valuation_rate) over (
                   partition by sle.item_code, sle.warehouse
                   order by sle.posting_ts, sle.name
                   rows between unbounded preceding and unbounded following) valuation_rate
        from g_item_code_warehouse t
        left join t_stock_ledger_entry sle on t.item_code = sle.item_code and t.warehouse = sle.warehouse
                                          and case when is_cancelled is null then 'No' else is_cancelled end = 'No'
                                          and sle.posting_ts <= t.max_posting_ts
    ) t2
)

-- *** Busca por code y ware con valor mayor a cero
, g_item_code_warehouse2 as (
    select item_code, warehouse, min_rownum
    from s_buy_amount_code_ware
    where coalesce(valuation_rate,0) = 0
)
, s_buy_amount_code_ware2 as (
    select t2.item_code, t2.warehouse, min_rownum,
           last_value(t2.valuation_rate) over (partition by t2.item_code
             order by t2.min_rownum desc rows between unbounded preceding and unbounded following) valuation_rate
    from (
        select distinct t.item_code, t.warehouse, min_rownum,
                last_value(sle.valuation_rate) over (
                   partition by sle.item_code --, sle.warehouse
                   order by sle.posting_ts, sle.name
                   rows between unbounded preceding and unbounded following) valuation_rate
        from g_item_code_warehouse2 t
        left join t_stock_ledger_entry sle on t.item_code = sle.item_code and t.warehouse = sle.warehouse
                                          and sle.valuation_rate > 0
    ) t2
)

-- *** Busca por code con valor mayor a cero
, g_item_code as (select item_code, min(min_rownum) min_rownum from s_buy_amount_code_ware2 where coalesce(valuation_rate,0) = 0 group by item_code)
, s_buy_amount_code as (
    select t2.item_code, min_rownum,
           last_value(t2.valuation_rate) over (partition by t2.item_code
             order by t2.min_rownum desc rows between unbounded preceding and unbounded following) valuation_rate
    from (
        select distinct t.item_code, min_rownum,
                last_value(sle.valuation_rate) over (
                   partition by sle.item_code
                   order by sle.posting_ts, sle.name
                   rows between unbounded preceding and unbounded following) valuation_rate
        from g_item_code t
        left join t_stock_ledger_entry sle on t.item_code = sle.item_code
                                          and sle.valuation_rate > 0
    ) t2
)

, s_buy_amount as (
    select a.item_code, a.warehouse,
           case when coalesce(a.valuation_rate,0) > 0 then a.valuation_rate
               when coalesce(b.valuation_rate,0) > 0 then b.valuation_rate
               else c.valuation_rate end buy_amount
    from s_buy_amount_code_ware a
    left join s_buy_amount_code_ware2 b on b.item_code = a.item_code and b.warehouse = a.warehouse
    left join s_buy_amount_code       c on c.item_code = a.item_code
)

-- *** Obtener el buy amount para los delivery notes
, s_buy_amount_dn as (
    select item_row, (
        select * from (
          select case when sle.voucher_type = 'Delivery Note' and sle.voucher_no = si2.delivery_note
                       and sle.voucher_detail_no = si2.dn_detail
                      then (
                             --previous_value
                             coalesce(lead(stock_value)
                                 over (partition by sle.item_code, sle.warehouse
                                        order by sle.item_code, sle.warehouse, sle.posting_ts desc, sle.name desc
                                 ),0)
                             -
                             stock_value -- actual_value
                           ) else null end buying_rate
          from t_stock_ledger_entry sle
          where sle.item_code = si2.item_code and sle.warehouse = si2.warehouse
      ) t where t.buying_rate is not null
    ) / qty as buying_amount
    from t_si si2 where dn_detail is not null
)

, non_stock as (
    select distinct a.item_code,
           last_value(a.base_rate / a.conversion_factor)
             over (
                partition by a.item_code
                order by a.modified
                rows between unbounded preceding and unbounded following
             ) item_rate
    from t_purchase_invoice_item a
    join t_purchase_invoice b on b.name = a.parent
    where b.company = ${map.company}
      and a.modified <= to_date(${map.to_date}, 'yyyy-mm-dd')
      and a.item_code in (select item_code from t_si where is_stock_item=0 group by item_code)
)
select
       si.parent as sales_invoice,
       customer,
       customer_group,
       posting_date,
       si.item_code,       si.warehouse,
       qty,
       base_net_amount / qty as avg_selling_rate,
       coalesce(ba.buy_amount, badn.buying_amount, ns.item_rate, 0) as avg_buying_rate,
       base_net_amount as selling_amount,
       coalesce(ba.buy_amount, badn.buying_amount, ns.item_rate, 0) * qty as buying_amount,
       base_net_amount - (coalesce(ba.buy_amount, badn.buying_amount, ns.item_rate, 0) * qty) as gross_profit,
       case when base_net_amount > 0
            then ((base_net_amount - (coalesce(ba.buy_amount, badn.buying_amount, ns.item_rate, 0) * qty)) / base_net_amount) * 100
            else 0 end as gross_profit_percent,
       si.cost_center,
       si.business_center
from t_si si
join t_item ti on ti.name = si.item_code
left join s_buy_amount ba on ba.item_code = si.item_code and ba.warehouse = si.warehouse and si.dn_detail is null and si.is_stock_item = 1
left join s_buy_amount_dn badn on badn.item_row = si.item_row and si.dn_detail is not null and si.is_stock_item = 1
left join non_stock ns on ns.item_code = si.item_code and si.is_stock_item = 0
order by posting_ts desc, si.item_row
"""
//		println query
		sql.execute(query)
		
	}
	
}
