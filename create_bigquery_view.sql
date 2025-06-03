CREATE OR REPLACE VIEW bigquery-dbt-project-baris.xomnia_dataset.t_player_counts_enriched AS

SELECT
player_counts.*,
app_info.type AS app_type,
app_info.name AS app_name,
app_info.releasedate AS release_date,
app_info.freetoplay AS is_free_to_play,
dev_info.developerfirm AS developer_firm,
app_lang_info.language_count AS count_of_language_available,
publisher_info.publisher AS publisher,
price_info.Initialprice AS original_price,
price_info.Discount / 100 AS discount_percentage

FROM bigquery-dbt-project-baris.xomnia_dataset.t_player_counts player_counts

INNER JOIN bigquery-dbt-project-baris.xomnia_dataset.t_app_info app_info # --> ENFORCE THE APPID FOREIGN KEY VIA INNER JOIN
ON player_counts.appid = app_info.appid

LEFT JOIN bigquery-dbt-project-baris.xomnia_dataset.t_dev_info dev_info
ON player_counts.appid = dev_info.appid

LEFT JOIN (SELECT DISTINCT(appid),  # --> otherwise it will create duplicates
                  language_count
           FROM `bigquery-dbt-project-baris.xomnia_dataset.t_app_languages_info`) app_lang_info
ON player_counts.appid = app_lang_info.appid

LEFT JOIN bigquery-dbt-project-baris.xomnia_dataset.t_publisher_info publisher_info
ON player_counts.appid = publisher_info.appid

LEFT JOIN bigquery-dbt-project-baris.xomnia_dataset.t_price_info price_info
ON player_counts.appid = price_info.appid
AND player_counts.date = price_info.date

;
