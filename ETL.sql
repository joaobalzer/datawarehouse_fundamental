--Staging
DROP TABLE IF EXISTS `upheld-dragon-348802.Postgres.northwind_public_stg_customers`;
DROP TABLE IF EXISTS `upheld-dragon-348802.Postgres.northwind_public_stg_employees`;
DROP TABLE IF EXISTS `upheld-dragon-348802.Postgres.northwind_public_stg_order_detail`;
DROP TABLE IF EXISTS `upheld-dragon-348802.Postgres.northwind_public_stg_orders`;
DROP TABLE IF EXISTS `upheld-dragon-348802.Postgres.northwind_public_stg_products`;
DROP TABLE IF EXISTS `upheld-dragon-348802.Postgres.northwind_public_stg_shippers`;
DROP TABLE IF EXISTS `upheld-dragon-348802.Postgres.northwind_public_stg_suppliers`;
--Dimension Tables
DROP TABLE IF EXISTS `upheld-dragon-348802.Postgres.northwind_public_dim_customers`;
DROP TABLE IF EXISTS `upheld-dragon-348802.Postgres.northwind_public_dim_employees`;
DROP TABLE IF EXISTS `upheld-dragon-348802.Postgres.northwind_public_dim_suppliers`;
DROP TABLE IF EXISTS `upheld-dragon-348802.Postgres.northwind_public_dim_shippers`;
DROP TABLE IF EXISTS `upheld-dragon-348802.Postgres.northwind_public_dim_products`;
--Fact Table
DROP TABLE IF EXISTS `upheld-dragon-348802.Postgres.northwind_public_fct_order_detail`;

--Staging Tables
CREATE TABLE IF NOT EXISTS
    `upheld-dragon-348802.Postgres.northwind_public_stg_customers` as (
        select
            country
            , city
            , fax
            , postal_code
            , address
            , region
            , customer_id
            , contact_name
            , phone
            , company_name
            , contact_title
        from `upheld-dragon-348802.Postgres.northwind_public_customers`
);

CREATE TABLE IF NOT EXISTS
    `upheld-dragon-348802.Postgres.northwind_public_stg_employees` as (
        select
            employee_id
            , first_name
            , last_name
            , country
            , city
            , postal_code
            , hire_date
            , extension
            , address
            , birth_date
            , region
            , photo_path
            , home_phone
            , reports_to
            , title
            , title_of_courtesy
            , notes

        from `upheld-dragon-348802.Postgres.northwind_public_employees`
    );

CREATE TABLE IF NOT EXISTS
    `upheld-dragon-348802.Postgres.northwind_public_stg_order_detail` as (
        select
              order_id
            , product_id
            , discount
            , unit_price
            , quantity
        from `upheld-dragon-348802.Postgres.northwind_public_order_details`
    );

CREATE TABLE IF NOT EXISTS
    `upheld-dragon-348802.Postgres.northwind_public_stg_orders` as (
        select
            order_id
            , employee_id
            , order_date
            , customer_id
            , ship_region
            , shipped_date
            , ship_country
            , ship_name
            , ship_postal_code
            , ship_city
            , freight
            , ship_via as shipper_id
            , ship_address
            , required_date
        from `upheld-dragon-348802.Postgres.northwind_public_orders`
    );


CREATE TABLE IF NOT EXISTS
    `upheld-dragon-348802.Postgres.northwind_public_stg_products` as (
        select
            product_id
            , product_name
            , units_in_stock
            , category_id
            , unit_price
            , quantity_per_unit
            , reorder_level
            , supplier_id
            , units_on_order
            ,
              case
                when discontinued = 0 then "No"
                else "Yes"
              end as is_discontinued
        from `upheld-dragon-348802.Postgres.northwind_public_products`
    );


CREATE TABLE IF NOT EXISTS
    `upheld-dragon-348802.Postgres.northwind_public_stg_shippers` as (
        select
            phone
            , company_name
            , shipper_id
        from `upheld-dragon-348802.Postgres.northwind_public_shippers`
    );


CREATE TABLE IF NOT EXISTS
    `upheld-dragon-348802.Postgres.northwind_public_stg_suppliers` as (
        select
            supplier_id
            ,  country
            ,  city
            ,  fax
            ,  postal_code
            ,  homepage
            ,  address
            ,  region
            ,  contact_name
            ,  phone
            ,  company_name
            ,  contact_title
        from `upheld-dragon-348802.Postgres.northwind_public_suppliers`
    );



--Dimension Tables

CREATE TABLE IF NOT EXISTS 
    `upheld-dragon-348802.Postgres.northwind_public_dim_customers` as (
        select
            row_number() over (order by customer_id) as customer_sk -- auto-incremental surrogate key
            , customer_id
            , country
            , city
            , fax
            , postal_code   
            , address
            , region
            , contact_name
            , phone
            , company_name
            , contact_title
        from `upheld-dragon-348802.Postgres.northwind_public_stg_customers`
);

CREATE TABLE IF NOT EXISTS 
    `upheld-dragon-348802.Postgres.northwind_public_dim_employees` as (
        select
            row_number() over (order by employee_id) as employee_sk -- auto-incremental surrogate key
            , employee_id
            , reports_to
            , first_name
            , last_name
            , country
            , city
            , postal_code
            , hire_date
            , extension
            , address
            , birth_date
            , region
            , photo_path
            , home_phone
            , title
            , title_of_courtesy
            , notes
        from `upheld-dragon-348802.Postgres.northwind_public_stg_employees`
    );

CREATE TABLE IF NOT EXISTS
    `upheld-dragon-348802.Postgres.northwind_public_dim_products` as (
        select
            row_number() over (order by product_id) as product_sk -- auto-incremental surrogate key
            , product_id
            , product_name
            , units_in_stock
            , category_id
            , unit_price
            , quantity_per_unit
            , reorder_level
            , supplier_id
            , units_on_order
            , is_discontinued
        from `upheld-dragon-348802.Postgres.northwind_public_stg_products`
);

CREATE TABLE IF NOT EXISTS
    `upheld-dragon-348802.Postgres.northwind_public_dim_shippers` as (
        select
            row_number() over (order by shipper_id) as shipper_sk -- auto-incremental surrogate key
            , phone
            , company_name
            , shipper_id
        from `upheld-dragon-348802.Postgres.northwind_public_stg_shippers`
    );




    
CREATE TABLE IF NOT EXISTS
`upheld-dragon-348802.Postgres.northwind_public_dim_suppliers` 
as ( with
    suppliers as (
        select *
        from `upheld-dragon-348802.Postgres.northwind_public_stg_suppliers`
    )
    , divisions as (
        select string_field_0 as country,
        string_field_1 as division
        from `upheld-dragon-348802.Postgres.seed_supplier_divisions`
    )
    , transformed as (
        select
            row_number() over (order by suppliers.supplier_id) as supplier_sk -- auto-incremental surrogate key
            , suppliers.supplier_id
            , suppliers.country
            , suppliers.city
            , suppliers.fax
            , suppliers.postal_code
            , suppliers.homepage
            , suppliers.address
            , suppliers.region
            , suppliers.contact_name
            , suppliers.phone
            , suppliers.company_name
            , suppliers.contact_title
            , divisions.division
        from suppliers
        left join divisions on suppliers.country = divisions.country
    )
    Select * from transformed
);


CREATE TABLE IF NOT EXISTS `upheld-dragon-348802.Postgres.northwind_public_fct_order_detail` as (
with
    customers as (
        select
        customer_sk
        , customer_id
        from `upheld-dragon-348802.Postgres.northwind_public_dim_customers`
    )
    , employees as (
        select
        employee_sk
        , employee_id
        from `upheld-dragon-348802.Postgres.northwind_public_dim_employees`
    )
    , suppliers as (
        select
        supplier_sk
        , supplier_id
        from `upheld-dragon-348802.Postgres.northwind_public_dim_suppliers`
    )
    , shippers as (
        select
          shipper_sk
        , shipper_id
        from `upheld-dragon-348802.Postgres.northwind_public_dim_shippers`
    )
    , products as (
        select
        product_sk
        , product_id
        from `upheld-dragon-348802.Postgres.northwind_public_dim_products`
    )
    , orders_with_sk as (
        select
            orders.order_id
            , employees.employee_sk as employee_fk
            , customers.customer_sk as customer_fk
            , shippers.shipper_sk as shipper_fk
            , orders.order_date
            , orders.ship_region
            , orders.shipped_date
            , orders.ship_country
            , orders.ship_name
            , orders.ship_postal_code
            , orders.ship_city
            , orders.freight
            , orders.ship_address
            , orders.required_date
        from `upheld-dragon-348802.Postgres.northwind_public_stg_orders` orders
        left join employees employees on orders.employee_id = employees.employee_id
        left join customers customers on orders.customer_id = customers.customer_id
        left join shippers shippers on orders.shipper_id = shippers.shipper_sk
    )
    , orders_detail_with_sk as (
        select
            order_dtl.order_id
            , products.product_sk as product_fk
            , order_dtl.discount
            , order_dtl.unit_price
            , order_dtl.quantity
        from `upheld-dragon-348802.Postgres.northwind_public_stg_order_detail` order_dtl
        left join products products on order_dtl.product_id = products.product_id
    )
    , final as (
        select
        order_dtl.order_id
        , orders.employee_fk
        , orders.customer_fk
        , orders.shipper_fk
        , orders.order_date
        , orders.ship_region
        , orders.shipped_date
        , orders.ship_country
        , orders.ship_name
        , orders.ship_postal_code
        , orders.ship_city
        , orders.freight
        , orders.ship_address
        , orders.required_date
        , order_dtl.product_fk
        , order_dtl.discount
        , order_dtl.unit_price
        , order_dtl.quantity
        from orders_with_sk orders
        left join orders_detail_with_sk order_dtl on orders.order_id = order_dtl.order_id
    )
   select * from final 
)

