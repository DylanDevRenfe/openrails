# MultiPlayerServer - reconnect & restore (dev branch)

Este directorio contiene cambios experimentales para añadir reconexión rápida (rejoin) y mejorar la fiabilidad del servidor multijugador.

Qué hace esta rama (resumen):

- Uso de colecciones thread-safe (ConcurrentDictionary) para conexiones activas.
- Broadcast asíncrono correctamente esperado (Task.WhenAll) en lugar de Parallel.ForEach con async lambda.
- Ventana de reconexión de 30s: cuando un jugador se desconecta, su sesión se mantiene temporalmente para permitir rejoin rápido.
- En el rejoin, el servidor notifica a los compañeros y solicita al currentServer el envío de un snapshot de estado (REQUEST_STATE) para que el cliente pueda ser reubicado.

Limitaciones actuales:

- El protocolo de rejoin es compatible hacia atrás: un cliente antiguo que no soporte token simplemente puede reconectar con su playerName y se intentará reasignar la sesión si está disponible dentro del TTL.
- No hay todavía un mecanismo completo de snapshot/restore del estado del tren desde el servidor; el servidor envía un "REQUEST_STATE" al currentServer para que éste reponga el estado del jugador reingresado. El soporte para que el currentServer responda con un snapshot debe implementarse/ajustarse en la parte cliente/servidor del motor de simulación.
- Para robustez a largo plazo recomendamos migrar el protocolo a framing binario + MessagePack/Protobuf y añadir versionado y checks de secuencia para switches y señales.

Cómo probar localmente (ejemplo):

1. Compilar MultiPlayerServer:

   msbuild Source\MultiPlayerServer\MultiPlayerServer.csproj /p:Configuration=Debug

2. Ejecutar el servidor:

   cd Source\MultiPlayerServer\bin\Debug
   MultiPlayerServer.exe 30000

3. Usar el cliente de prueba incluido (TestClient) para simular desconexión/reconexión rápida.

Siguientes pasos recomendados:

- Implementar el envío de snapshot por parte del servidor de juego (currentServer) al recibir REQUEST_STATE.
- Implementar versionado para switches/signals y aplicar server-authoritative changes con sequence numbers.
- Migrar a framing binario y un esquema tipado de mensajes.

