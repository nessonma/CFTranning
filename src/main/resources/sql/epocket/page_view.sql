-- 图书 2、文献 3、指南 1
select distinct_id,
case when page = '指南详情页' then '1' when page = '图书详情页' then '2' when page = '文献详情页' then '3' end,
item_nid, count(1) from events where event = 'page_view'
and (page = '图书详情页' or page = '指南详情页' or page = '文献详情页')
and (distinct_id like 'u:%%' or distinct_id like 'd:%%' )
and `date` between '2018-09-03' and '2018-09-09'
group by distinct_id, page, item_nid

-- 云学院 5 、帖子 4
select distinct_id,
case when page = 'discuss_detail' then '4' when page = 'group' then '5' end,
case page when 'discuss_detail' then share_uid when 'group' then group_uid end as item_nid,  count(1) from events where event = 'page_view'
and ((page = 'discuss_detail' and item_type = '公开讨论帖') or (page = 'group' and type = '公开的'))
and `date` between '2018-09-03' and '2018-09-09'
and (distinct_id like 'u:%%' or distinct_id like 'd:%%' )
group by distinct_id, page, item_nid;

-- 测试数据
select distinct_id, case when page = '指南详情页' then '1' when page = '图书详情页' then '2' when page = '文献详情页' then '3' when page = 'discuss_detail' then '4' when page = 'group' then '5' end,
case page when 'discuss_detail' then share_uid when 'group' then group_uid else item_nid end as `item_nid`,
count(1) as `click` from events where event = 'page_view'
and (page = '图书详情页' or page = '指南详情页' or page = '文献详情页' or (page = 'discuss_detail' and item_type = '公开讨论帖') or (page = 'group' and type = '公开的'))
and (distinct_id like 'u:%%' or distinct_id like 'd:%%' )
and `date` between '2018-09-03' and '2018-09-10'
group by distinct_id, page, item_nid
