# Proyecto Python

Este proyecto contiene un script principal que utiliza varias funciones para trabajar con el registro de clientes ABA y tráfico.

## Las opciones de línea de comandos son las siguientes:

- `ABA`: Si esta opción es seleccionada, el script espera un argumento adicional que es el nombre del archivo. Luego, obtiene los datos de ABA del archivo y actualiza la base de datos de ABA.
- `TRAFFIC`: Si esta opción es seleccionada, el script espera dos argumentos adicionales que son el primer y último día. Luego, actualiza los datos de tráfico en la base de datos para ese rango de fechas.
- `TRAFFIC_ABA`: Si esta opción es seleccionada, el script actualiza los datos de tráfico de ABA en la base de datos.

## Uso

Para utilizar este script, simplemente ejecútalo desde la línea de comandos con las opciones y argumentos adecuados. Por ejemplo:

```bash
python script.py ABA filename.xlsx

```
