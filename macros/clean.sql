{% macro trim_clean(col)%}
    TRIM({{col}})
{%endmacro%}

{%macro text_clean(col,case_type='none')%}
    {%if case_type=='upper'%}
        UPPER(TRIM({{col}}))
    {%elif case_type=='lower'%}
        LOWER(TRIM({{col}}))
    {%elif case_type=='initcap'%}
        INITCAP(TRIM({{col}}))
    {%else%}
        TRIM({{col}})
    {%endif%}
{%endmacro%}
{% macro numeric_clean(col)%}
    TRY_TO_NUMBER(REGEXP_REPLACE({{col}},'[^0-9.]',''))
{%endmacro%}
{% macro number(col,default_value=0)%}
    COALESCE(TRY_TO_NUMBER({{col}}),{{default_value}})
{%endmacro%}
{%macro datw(col)%}
    COALESCE(
        TRY_TO_DATE({{col}},'DD-MM-YYYY'),
        TRY_TO_DATE({{col}},'YYYY-MM-DD'),
        TRY_TO_DATE({{col}},'MM/DD/YYYY')
    )
{%endmacro%}
{% macro timestamp(col)%}
    TRY_TO_TIMESTAMP({{col}})
{%endmacro%}
{% macro email_clean(col)%}
    LOWER(TRIM({{col}}))
{%endmacro%}
{%macro email_validate(col)%}
    CASE
        WHEN LOWER(TRIM({{col}})) RLIKE '^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}$'
        THEN LOWER(TRIM({{col}}))
        ELSE NULL
    END
{%endmacro%}
{%macro phone_clean(col)%}
    REGEXP_REPLACE({{col}},'[^0-9]','')
{%endmacro%}
{%macro phone_validate(col)%}
    CASE
        WHEN LENGTH({{col}})=11 and left({{col}},1)='1'
            THEN SUBSTRING({{col}},2)
        WHEN LENGTH({{col}})=10
            THEN {{col}}
        ELSE NULL
    END 
{%endmacro%}
{%macro standardized_role(col)%}
    CASE
        WHEN LOWER({{col}}) like '%sales associate%' then 'Assosciate'
        WHEN LOWER({{col}}) like '%store manager%' then 'Manager'
        WHEN LOWER({{col}}) like '%senior manager%' then 'Senior Manager'
        ELSE INITCAP(TRIM({{col}}))
    END 
{%endmacro%}
{%macro cal_duration(start_date,end_date)%}
    CASE
        WHEN {{start_date}} is not null and {{end_date}} is not null
        then DATEDIFF(day, {{start_date}},{{end_date}})
        else NULL 
    END
{%endmacro%}
{%macro cal_roi(revenue,cost)%}
    case 
        when {{cost}}>0 then 
            ({{revenue}}-{{cost}})/nullif({{cost}},0)
        else null 
    end 
{%endmacro%}
{%macro profit_amount(total_amount, total_cost, total_discount, shipping_cost, tax_amount)%}
    ({{total_amount}})
    -{{total_cost}}
    -{{total_discount}}
    -{{shipping_cost}}
    -{{tax_amount}}
{%endmacro%}
{%macro profit(total_amount, total_cost, total_discount, shipping_cost, tax_amount)%}
            (
                {{total_amount}}
                -{{total_cost}}
                -{{total_discount}}
                -{{shipping_cost}}
                -{{tax_amount}}
            )
{%endmacro%}
{%macro age_cal(birth_date)%}
    case 
        when {{birth_date}} is not null 
        then datediff('year',{{birth_date}},CURRENT_DATE)
        else null 
    end 
{%endmacro%}
{% macro customer_segment(age)%}
    case 
        when {{age}} is null then 'Unknown' 
        when {{age}} between 18 and 35 then 'Young'
        when {{age}} between 36 and 55 then 'Middle-aged'
        when {{age}}>55 then 'Senior'
        else 'Unknown'
    end 
{%endmacro%}
{%macro order_time(order_date)%}
    case 
        when DATE_PART(hour,{{order_date}}) between 5 and 11 then 'Morning'
        when DATE_PART(hour,{{order_date}}) between 12 and 16 then 'Afternoon'
        when DATE_PART(hour,{{order_date}}) between 17 and 21 then 'Evening'
        else 'Night'
    end 
{%endmacro%}
{%macro delivery_status(delivery_date,estimated_delivery_date)%}
    CASE
        when {{delivery_date}} is not null and 
        {{delivery_date}} <= {{estimated_delivery_date}}
            then 'On Time'
        when {{delivery_date}} is not null 
            and {{delivery_date}} > {{estimated_delivery_date}}
            then 'Delayed'
        when {{delivery_date}} is null 
            and CURRENT_DATE > {{estimated_delivery_date}}
            then 'Potentially Delayed'
        else 'In Transit'
    END
{%endmacro%}
{%macro divide(numerator, denominator)%}
    case 
        when {{denominator}}=0 then null 
        else {{numerator}}/{{denominator}}
    end
{%endmacro%}