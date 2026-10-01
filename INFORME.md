# Informe

Las instancias de Sum leen de una misma cola y se reparten los registros. Cada consulta tiene un identificador `query_id`, generado en el gateway, que permite mantener separados los datos de los clientes aunque se procesen al mismo tiempo.

Cuando termina el envío de un cliente, el gateway informa cuántos registros envió. La instancia de Sum que recibe ese aviso manda un `DRAIN` a todas las instancias de Sum, incluida ella misma. Cada una publica sus acumulados y luego informa cuántos registros representan. Los registros que se procesan después de `DRAIN` se envían como actualizaciones, también con su progreso. Cada lote y su progreso se identifican por instancia y secuencia para ignorar duplicados en Aggregation. La consulta puede finalizar cuando se recibió el progreso inicial de todos los Sum y la suma de los conteos coincide con el total enviado por el cliente.

Para repartir las frutas entre Aggregation uso un hash determinístico (FNV-1a) del nombre de cada fruta. El resto de dividirlo por la cantidad de instancias indica a cuál enviar los datos. Así, una misma fruta siempre llega al mismo Aggregation y sus datos no se replican entre particiones. Sólo se envia la información de progreso, necesaria para que cada Aggregation evalúe la condición de finalización. Cada uno calcula un top parcial y Join espera uno de cada instancia para obtener el top final.

Para atender varios clientes se guarda el estado por consulta. Sum acumula por fruta, por lo que la memoria de los acumulados depende de las frutas distintas y las consultas activas. 
Tener más Sum permiten repartir el procesamiento de registros y más Aggregation permiten repartir las frutas.
