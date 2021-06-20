# HTML Scraper

#### Gabriel Fernando Martín Fernández C411 
#### Miguel Alejandro Asin Barthemy C411 

## Resumen

Una implementación de un scraper distribuido empleando python. El scraper implementado utiliza un anillo chord para la coordinación entre los componentes remotos y este es la base principal de su funcionamiento en la red.  Una instancia completa de la aplicación lo constituyen 4 componentes principales diferenciados por sus funciones: la base de datos, la interfaz, el scraper y el nodo de chord. Para la comunicación entre componentes remotos se emplearon las herramientas proporcionadas por la librería aiomas que a su vez utiliza fuertemente las herramientas de asyncio. 

Entre las principales características implementadas aparte de las necesarias para el scraper básico se incluyen: un cierto grado de resistencia a errores que permite que la aplicación funcione correctamente y sin pérdida de datos a pesar de desconectarse uno de los nodos repentinamente; los nodos desconectados por errores al volver a ser accesibles pueden continuar funcionando dentro de la aplicación; la base de datos adjunta sirve como un backup extra de la información y se guarda localmente permitiendo después que un nodo pueda cargar los datos que esta contiene al inicializarse.

## Como usar

Antes de crear la imagen de docker es necesario tener una imagen local de python.

```
docker pull docker.uclv.cu/python
```

Para crearla se necesitan descargar 10 MB.

```
docker build -t <nombre de la imagen> .
docker network create <nombre de la red>
```

Crear el contenedor docker e iniciar los clientes en este orden

```
docker container run -it --rm --network <nombre de la red> <nombre de la imagen> bash
python3 bd_client.py &
python3 sc_client.py &
python3 ch_client.py &
python3 fz_client.py
```
Para crear un segundo nodo y conectarlo al primero, se inician en un segundo contenedor docker los mismos clientes, con una pequeña modificación en el tercero

```
python3 bd_client.py &
python3 sc_client.py &
python3 ch_client.py --address <ip del contededor a quien se va a conectar>:7700 &
python3 fz_client.py
```

## Detalles del funcionamiento
El sistema está diseñado para que la aplicación este compuesta por los cuatro clientes inicializados anteriormente y se conecte a instancias similares de esta ubicadas en direcciones remotas. El usuario realiza las peticiones de urls desde la interfaz la cual se encarga de pasarle el request al cliente de chord. Este cliente inicializa el nodo chord que realiza las tareas principales. Primeramente el nodo chord decide si la responsabilidad del url que se pide le corresponde a el u a otro nodo y la petición se distribuye hasta llegar al responsable. El nodo al cual le corresponde el url pedido chequea si lo tiene guardado en cuyo caso lo retorna, de no tenerlo se conecta al nodo de su scraper correspondiente y le pide que le devuelva el html y finalmente procede a retornarlo.

El anillo chord guarda réplicas de las informaciones obtenidas de manera que siempre haya al menos 2 nodos con un determinado html que ya fue scrapeado anteriormente. De esta forma si un nodo se cae o se desconecta repentinamente se asegura que exista al menos otro con la información que este tenía y para evitar otro fallo se procede a crear otra réplica de la información. Los nodos que sean inalcanzables en un determinado momento son marcados de cierta forma por el anillo y este se comporta momentaneamente como si no existiesen hasta que pueda reconectarse nuevamente. Para esto último se agregó una rutina para actualizar el estado de los nodos.

La base de datos es empleada para guardar los htmls de los cuales es responsable su nodo chord correspondiente sirviendo como una salva permanente de los datos. Al inicializar el cliente de chord es posible asignarle una bd que ya exista, en cuyo caso se cargan los datos guardados en esta o se crea una nueva según se configure. 

## Ficheros

bd_client.py -> Cliente de la base de datos. 

ch_client.py -> Cliente de chord.

fz_client.py -> Cliente de la interfaz.

sc_client.py -> Cliente del scraper.

Nodes/bd.py -> Nodo encargado de la base de datos.

Nodes/chord.py -> Nodo de chord.

Nodes/interface.py -> Interfaz principal de la aplicación.

Nodes/logger.py -> Logger para registrar eventos y debuguear.

Nodes/scrapper.py -> Nodo encargado de scrapear

Nodes/utils.py -> Herramientas extras

## Ejemplo

Para visualizar el funcionamiento del sistema, modifique el archivo logger.py; tal que la salida del logger sea por la consola y no hacia un fichero.
Comience ejecutando en un contenedor docker, los dados anteriormente.

```
docker container run -it --rm --network <nombre de la red> <nombre de la imagen> bash
```
```
python3 bd_client.py &
```
```
python3 sc_client.py &
```
```
python3 ch_client.py &
```
```
python3 fz_client.py
```

Realice el pedido de una URL en el formato correcto:

```
https://evea.uh.cu
```

En un segundo contenedor se inicia un segundo nodo, que se conectará al anterior

```
docker container run -it --rm --network <nombre de la red> <nombre de la imagen> bash
```
```
python3 bd_client.py &
```
```
python3 sc_client.py &
```
```
python3 ch_client.py --address <IP del primer contenedor>:7700 &
```
```
python3 fz_client.py
```

Se realiza un segundo request:

```
https://google.com
```

En dependencia del ID asignado a cada nodo, uno de los nodos anteriores será el encargado de realizar el scrapper. Después de encontrado el código HTML correspondiente se almacena en la BD del nodo que hizo el scrapper y es posible acceder a él aún y cuando no se tenga acceso a las páginas oficiales de las URL anteriores:

```
Quitar acceso a internet y preguntar en cada nodo por las mismas URL
```

Para comprobar la tolerancia a fallas, detenga el primer contenedor, tal que dicho nodo deje el anillo sin enviar un "disconnection request" que permita a los otros nodos actualizar el estado del sistema e intente nuevamente realizar las mismas peticiones de URL.

Como la información de cada nodo n se almacena en pred(n), y el sistema es capaz de recuperar el correcto funcionamiento de la red chord, tras la salida repentina de n del sistema, la informaci[on que este almacenaba aun sigue siendo accesible, debido a la replicación de la misma en otro nodo.

Conecte un tercer nodo al sistema:

```
docker container run -it --rm --network <nombre de la red> <nombre de la imagen> bash
```
```
python3 bd_client.py &
```
```
python3 sc_client.py &
```
```
python3 ch_client.py --address <IP del contenedor restante>:7700 &
```
```
python3 fz_client.py
```

En caso de que el nodo n deje voluntariamente el sistema con el comando:

```
exit
```

La información de succ(n) pasa para pred(n) sin que este último modifique la que ya tiene. Lo cual permite mantener la flexibilidad del sistema ante la conexión y desconexión de cualquier nodo. Como toda la información es accesible siempre que un nodo esté funcionando (y se asume que siempre habrá algún nodo funcionando), se tiene una disponibilidad del 100%. Como los nodos son independientes y la caída repentina de uno de ellos no afecta el funcionamiento de los otros, consituye un sistema confiable y seguro. En el caso de que el último nodo operativo falle, la información queda almacenada en su BD, por lo que un reinicio del mismo basta para poner nuevamente operativo al sistema.
