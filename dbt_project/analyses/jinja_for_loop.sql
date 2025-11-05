{%-  set cars=["BMW","Benz","Skoda","Suzuki","Ferrari"] -%}

{%- for i in cars -%}

 {%- if i != "Suzuki" -%}
    {{ i }} is a good car 

{%- else -%}
    {{ i }} is an average car
 {% endif %}


{% endfor %}