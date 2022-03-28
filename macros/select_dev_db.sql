{% macro select_dev_db(database_nm) -%}

    {%- if target.name == 'dev' -%}

        {{ database_nm }}

    {%- else -%}

        {{ database_nm | trim }}_DEV

    {%- endif -%}

{%- endmacro %}