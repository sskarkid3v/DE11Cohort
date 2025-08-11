
      update "salesdb"."snapshots"."customers_scd2"
    set dbt_valid_to = DBT_INTERNAL_SOURCE.dbt_valid_to
    from "customers_scd2__dbt_tmp155519506446" as DBT_INTERNAL_SOURCE
    where DBT_INTERNAL_SOURCE.dbt_scd_id::text = "salesdb"."snapshots"."customers_scd2".dbt_scd_id::text
      and DBT_INTERNAL_SOURCE.dbt_change_type::text in ('update'::text, 'delete'::text)
      
        and "salesdb"."snapshots"."customers_scd2".dbt_valid_to is null;
      


    insert into "salesdb"."snapshots"."customers_scd2" ("customer_nk", "first_name", "last_name", "email", "city", "country", "signup_ts", "dbt_updated_at", "dbt_valid_from", "dbt_valid_to", "dbt_scd_id")
    select DBT_INTERNAL_SOURCE."customer_nk",DBT_INTERNAL_SOURCE."first_name",DBT_INTERNAL_SOURCE."last_name",DBT_INTERNAL_SOURCE."email",DBT_INTERNAL_SOURCE."city",DBT_INTERNAL_SOURCE."country",DBT_INTERNAL_SOURCE."signup_ts",DBT_INTERNAL_SOURCE."dbt_updated_at",DBT_INTERNAL_SOURCE."dbt_valid_from",DBT_INTERNAL_SOURCE."dbt_valid_to",DBT_INTERNAL_SOURCE."dbt_scd_id"
    from "customers_scd2__dbt_tmp155519506446" as DBT_INTERNAL_SOURCE
    where DBT_INTERNAL_SOURCE.dbt_change_type::text = 'insert'::text;

  