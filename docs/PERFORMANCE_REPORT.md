# Reporte de rendimiento (Fase 3)

Prueba: `ThroughputLoadTest` (`mvn test -Pload`), 2 000 eventos `DEPOSIT` enviados con 1 000 peticiones en vuelo, mercado simulado de **10 ms fijos** (bloqueante, en hilos virtuales). Cada escenario además **verifica la corrección**: la suma de los balances de todas las cuentas debe ser exactamente 2 000.

Equipo: macOS (Apple Silicon), JDK 21 (Microsoft OpenJDK 21.0.12). Los números absolutos dependen de la máquina; lo relevante es la **proporción** entre escenarios.

| Escenario | Lanes | Cuentas | Tiempo (ms) | Throughput (ev/s) | p50 (ms) | p95 (ms) | p99 (ms) |
|---|---:|---:|---:|---:|---:|---:|---:|
| **Línea base: lock global (1 lane)** | 1 | 1000 | 25 598 | **78** | 12 525 | 12 991 | 13 063 |
| 16 lanes | 16 | 1000 | 1 988 | 1 006 | 610 | 1 079 | 1 180 |
| 64 lanes | 64 | 1000 | 877 | 2 279 | 212 | 549 | 597 |
| **256 lanes** | 256 | 1000 | 241 | **8 297** | 53 | 135 | 158 |
| 64 lanes, 100 cuentas | 64 | 100 | 1 488 | 1 343 | 307 | 806 | 1 003 |
| Cuenta caliente (1 cuenta) | 64 | 1 | 25 037 | 80 | 12 471 | 12 551 | 12 558 |

## Conclusiones

1. **Escalabilidad:** pasar de serializar todo (lock global, 78 ev/s) a serializar solo por cuenta con 256 lanes da **~106× más throughput** y reduce el p99 de ~13 s a ~160 ms, sin perder corrección (balances exactos en todos los escenarios).
2. **La línea base coincide con la teoría:** con una sola lane el throughput es `1 / latencia ≈ 1 / 12.8 ms ≈ 78 ev/s`.
3. **Cuenta caliente (límite del diseño):** si todo el tráfico va a una sola cuenta el throughput vuelve a ~80 ev/s, sin importar cuántas lanes haya. Es consecuencia de la regla de negocio (el orden y la exclusión por cuenta son necesarios para que el balance sea correcto). Mitigaciones posibles (no implementadas): agrupar eventos consecutivos de la misma cuenta en un solo `compute` (microbatching) y reducir la latencia del mercado.
4. **Por qué 64 lanes no da 64× la línea base:** al repartir pocas cuentas en pocas lanes por hash, la lane más cargada marca el tiempo total (problema de "bolas en cajas"). Con 1 000 cuentas en 64 lanes la lane más cargada recibe bastante más que el promedio; con 100 cuentas el desbalance es mayor (1 343 ev/s). Más lanes que cuentas activas reduce las colisiones, por eso 256 lanes escala mejor. **Decisión:** las lanes son baratas (una cola y un contador), así que conviene que `app.concurrency.lanes` sea varias veces mayor al número de cuentas activas concurrentes; se ajusta sin recompilar (`APP_CONCURRENCY_LANES`).
5. **Latencias p50/p95 altas en esta prueba:** el cliente de la prueba lanza 1 000 peticiones de golpe, por lo que la mayor parte del tiempo medido es **espera en cola** (la saturación es deliberada). En producción, ese mismo mecanismo está acotado: al llenarse la cola de una lane se responde `429` (`OverloadApiIntegrationTest`).

## Cómo reproducir

```bash
mvn test -Pload            # imprime la tabla y la guarda en target/load-report.md
```
