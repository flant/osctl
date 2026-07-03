# osctl — ES5 full-prefix snapshot chart (werf)

werf-проект с тремя cronjob'ами osctl для legacy-кластера **Elasticsearch 5.x** в режиме `full_prefix_snapshots`:

- `30-snapshots` — создание снапшотов `<prefix>-<дата>` (все открытые индексы префикса);
- `31-snapshotschecker` — проверка свежести/полноты снапшотов + алерт в Madison;
- `32-snapshotsdelete` — ретеншн по дням.

Job `snapshotsbackfill` намеренно не включён — в этом режиме он отключён.

## Структура

```
├── werf.yaml
├── values.yaml
└── templates/
    ├── _helpers.yaml
    ├── 00-configmap.yaml
    ├── 30-snapshots-cronjob.yaml
    ├── 31-snapshotschecker-cronjob.yaml
    └── 32-snapshotsdelete-cronjob.yaml
```

## Параметры (`values.yaml`)

| Параметр | Назначение | Значение под изученный кластер |
|----------|------------|--------------------------------|
| `opensearchUrl` | адрес ES | `http://10.5.6.9:9200` |
| `snapshotRepo` | репозиторий снапшотов | `s3-backup` |
| `madisonKey` / `osdUrl` | алерты Madison | (заполнить) |
| `maxConcurrentSnapshots` | сколько снапшотов создавать одновременно | `1` (см. ниже) |
| `schedules.*` | расписания cron | 30/31/32 |
| `registrySecret` | imagePullSecret | `registrysecret` |
| `nodeSelector` / `affinity` / `tolerations` | размещение подов | prod-worker (spb2) |
| `indicesConfig` | содержимое `osctlindicesconfig.yml` (префиксы + сроки в днях) | 9 префиксов из кластера |

**`maxConcurrentSnapshots` на ES5 = 1.** В Elasticsearch 5.x одновременно может выполняться только один снапшот на кластер/репозиторий — попытка второго даёт `concurrent_snapshot_execution_exception`. Значение > 1 приведёт к ошибкам (параллельные снапшоты появились только в ES 7.x).

Префиксы в `indicesConfig` соответствуют реальным неймспейсам кластера: `as-avataria-mm/vk/ok/gm`, `as-horror`, `as-avaland-v2`, `as-stories`, `as-tgc`, `as-tropicania-vk`. `snapshot_count_s3` — сколько **дней** хранить снапшоты.

## Предусловие

S3-репозиторий (`snapshotRepo`) должен быть заранее зарегистрирован в кластере — osctl репозитории не создаёт.
