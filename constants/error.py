CLI_TRAFFIC_ERROR = """Necesitas pasar el firstday y lastday como argumento
Ejemplo: update-db traffic 20240101 20240107
"""

CLI_ABA_ERROR = """Necesitas pasar el directorio del reporte Registro ABA como argumento
Ejemplo: update-db aba "/home/fooziman/documents/Clientes_ABA_Registros_FE22022024.xlsx" "/home/fooziman/documents/Totales_Por_Nodos_FE22022024.xlsx"
"""

CLI_ARGVS_EMPTY_ERROR = """Se requiere utilizar uno de los siguientes argumentos: aba, aba-total, traffic o traffic-aba
Ejemplo: update-db traffic-aba
"""
