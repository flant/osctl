# Переменные окружения и флаги

Этот документ описывает переменные окружения и флаги для osctl.

## Порядок приоритета

Конфигурация загружается в следующем порядке:
1. **Флаги командной строки**
2. **Переменные окружения**
3. **Общий конфиг файл** (`config.yaml`)
4. **Файл конфигурации индексов** (`osctlindicesconfig.yaml`) - для команд snapshot, indicesdelete, snapshotsdelete, snapshotchecker
5. **Значения по умолчанию** (наименьший приоритет)

## Общие флаги

Эти флаги доступны для всех команд:

| Флаг | Переменная окружения | Описание | Значение по умолчанию |
|------|---------------------|----------|--------------|
| `--action` | `OSCTL_ACTION` | Автоматически выполнить команду (snapshot, retention, dereplicator, etc.) | (пусто) |
| `--config` | `OSCTL_CONFIG` | Путь к файлу конфигурации | `config.yaml` |
| `--os-url` | `OPENSEARCH_URL` | URL OpenSearch | `https://opendistro:9200` |
| `--os-recoverer-url` | `OPENSEARCH_RECOVERER_URL` | URL OpenSearch Recoverer | `https://opendistro-recoverer:9200` |
| `--cert-file` | `OPENSEARCH_CERT_FILE` | Путь к сертификату | `/etc/ssl/certs/admin-crt.pem` |
| `--key-file` | `OPENSEARCH_KEY_FILE` | Путь к приватному ключу | `/etc/ssl/certs/admin-key.pem` |
| `--ca-file` | `OPENSEARCH_CA_FILE` | Путь к CA | (пусто) |
| `--timeout` | `OPENSEARCH_TIMEOUT` | Таймаут запросов | `300s` |
| `--retry-attempts` | `OPENSEARCH_RETRY_ATTEMPTS` | Количество повторных попыток для запросов в апи | `3` |
| `--date-format` | `OPENSEARCH_DATE_FORMAT` | Формат даты в названиях индексов и снапшотов | `%Y.%m.%d` |
| `--recoverer-date-format` | `RECOVERER_DATE_FORMAT` | Формат даты для индексов у Recoverer | `%d-%m-%Y` |
| `--madison-url` | `MADISON_URL` | URL API Madison | `https://madison.flant.com/api/events/custom/` |
| `--madison-key` | `MADISON_KEY` | Ключ API Madison | (пусто) |
| `--osd-url` | `OPENSEARCH_DASHBOARDS_URL` | URL OpenSearch Dashboards | (пусто) |
| `--osctl-indices-config` | `OSCTL_INDICES_CONFIG` | Путь к конфигу индексов - для snapshot, indicesdelete, snapshotsdelete, snapshotchecker | `osctlindicesconfig.yaml` |
| `--dry-run` | `DRY_RUN` | Показать что будет сделано без выполнения | `false` |
| `--snap-repo` | `SNAPSHOT_REPOSITORY` | Название репо для снапшотов | (пусто) |

## Параметр action

Параметр `action` позволяет автоматически выполнить нужную команду без указания её в командной строке. Сделано для деплойментов и кронджоб в Kubernetes.

### Доступные значения action:
- `snapshots` 
- `snapshot-manual` 
- `snapshotsdelete` 
- `snapshotschecker` 
- `snapshotsbackfill`
- `indicesdelete` 
- `retention` 
- `dereplicator`
- `coldstorage` 
- `extracteddelete`
- `danglingchecker`
 - `sharding`
 - `indexpatterns`
 - `datasource`

### Примеры использования:

**В config.yaml:**
```yaml
action: "snapshot"
opensearch_url: "https://opendistro:9200"
# ...
```

**Через переменную окружения:**
```bash
export OSCTL_ACTION=retention
osctl
```

**Через флаг:**
```bash
osctl --action=snapshot
```

## Флаги команд

### `coldstorage`

Перемещает индексы на узлы cold, если они старше N дней.

| Флаг | Переменная окружения | Описание | Значение по умолчанию |
|------|---------------------|----------|--------------|
| `--cold-attribute` | `COLD_ATTRIBUTE` | Атрибут узлов для cold | (пусто) |
| `--hot-count` | `HOT_COUNT` | Количество дней, которые нужно держать индексы в hot | `4` |
| `--dry-run` | `DRY_RUN` | Показать изменения без их применения | `false` |

**Ключи в конфиг файле:**
- `cold_attribute`
- `hot_count`

### `retention`

Удаляет старые индексы при превышении порога использования диска.

| Флаг | Переменная окружения | Описание | Значение по умолчанию |
|------|---------------------|----------|--------------|
| `--retention-threshold` | `RETENTION_THRESHOLD` | Порог использования диска в процентах | `75` |
| `--retention-days-count` | `RETENTION_DAYS_COUNT` | Количество дней для хранения индексов | `2` |
| `--retention-check-snapshots` | `RETENTION_CHECK_SNAPSHOTS` | Проверять наличие валидных снапшотов перед удалением | `true` |
| `--retention-check-nodes-down` | `RETENTION_CHECK_NODES_DOWN` | Проверять выбывшие ноды из кластера перед запуском retention | `true` |
| `--dry-run` | `DRY_RUN` | Показать, какие индексы будут удалены, без удаления | `false` |

**Ключи в конфиг файле:**
- `retention_threshold`
- `retention_days_count`
- `retention_check_snapshots`
- `retention_check_nodes_down`

### `dereplicator`

Уменьшает количество реплик до 0 для индексов старше указанного количества дней.

| Флаг | Переменная окружения | Описание | Значение по умолчанию |
|------|---------------------|----------|--------------|
| `--dereplicator-days-count` | `DEREPLICATOR_DAYS` | Какое количество дней держать индексы с репликами | `2` |
| `--dereplicator-use-snapshot` | `DEREPLICATOR_USE_SNAPSHOT` | Проверять снапшоты перед уменьшением реплик | `false` |
| `--dry-run` | `DRY_RUN` | Показать изменения числа реплик без применения | `false` |

**Ключи в конфиг файле:**
- `dereplicator_days_count`
- `dereplicator_use_snapshot`

### `extracteddelete`

Удаляет extracted индексы, которые больше не нужны.

| Флаг | Переменная окружения | Описание | Значение по умолчанию |
|------|---------------------|----------|--------------|
| `--days` | `EXTRACTED_DAYS` | Какое количество дней держать extracted индексы | `7` |
| `--extracted-pattern` | `EXTRACTED_PATTERN` | Префикс для extracted индексов | `extracted_` |
| `--opensearch_recoverer_url` | `OPENSEARCH_RECOVERER_URL` | Url рековерера | `https://opendistro-recoverer:9200` |
| `--recoverer_date_format` | `RECOVERER_DATE_FORMAT` | Формат даты у extracted индексов | `%d-%m-%Y` |
| `--dry-run` | `DRY_RUN` | Показать удаляемые extracted индексы без удаления | `false` |

**Ключи в конфиг файле:**
- `opensearch_recoverer_url`
- `extracted_pattern`
- `extracted_days`
- `recoverer_date_format`

### `snapshots`, `snapshot-manual`

Создает снапшоты индексов.

**Специальные флаги для snapshot-manual:**
| Флаг | Переменная окружения | Описание | Значение по умолчанию |
|------|---------------------|----------|--------------|
| `--snapshot-manual-kind` | `SNAPSHOT_KIND` | Тип паттерна: prefix или regex | `prefix` |
| `--snapshot-manual-value` | `SNAPSHOT_VALUE` | Значение паттерна | (пусто) |
| `--snapshot-manual-name` | `SNAPSHOT_NAME` | Имя снапшота (обязательно для regex) | (пусто) |
| `--snapshot-manual-system` | `SNAPSHOT_SYSTEM` | Флаг системного индекса (получает индексы с точкой, независимо от даты) | `false` |
| `--snapshot-manual-repo` | `SNAPSHOT_MANUAL_REPO` | Переопределить репозиторий для manual снапшота | (пусто) |
| `--dry-run` (только для `snapshots`) | `DRY_RUN` | Показать создаваемые снапшоты без выполнения | `false` |

### `sharding`

Рассчитывает шардирование и создает/обновляет index template.

| Флаг | Переменная окружения | Описание | Значение по умолчанию |
|------|---------------------|----------|--------------|
| `--sharding-target-size-gib` | `SHARDING_TARGET_SIZE_GIB` | Целевой размер шарда (GiB, максимум 50) | `25` |
| `--exclude-sharding` | `EXCLUDE_SHARDING` | Регекс для исключения паттернов | (пусто) |
| `--sharding-routing-allocation-temp` | `SHARDING_ROUTING_ALLOCATION_TEMP` | Значение для `routing.allocation.require.temp` (например, `hot`) | (пусто) |
| `--dry-run` | `DRY_RUN` | Показать создаваемые/обновляемые шаблоны без применения | `false` |

**Ключи в конфиг файле:**
- `sharding_target_size_gib`
- `exclude_sharding`
- `sharding_routing_allocation_temp`

### `indexpatterns`

Управляет index patterns в Kibana через OpenSearch API (индекс `.kibana`)

| Флаг | Переменная окружения | Описание | Значение по умолчанию |
|------|---------------------|----------|--------------|
| `--kibana-index-regex` | `KIBANA_INDEX_REGEX` | Регекс для построения паттернов | `^([\\w-]+)-([\\w-]*)(\\d{4}[\\.-]\\d{2}[\\.-]\\d{2}(?:[\\.-]\\d{2})*)$` |
| `--indexpatterns-kibana-multitenancy` | `INDEXPATTERNS_KIBANA_MULTITENANCY` | Режим multitenancy | `false` |
| `--indexpatterns-kibana-tenants-config` | `INDEXPATTERNS_KIBANA_TENANTS_CONFIG` | Путь к YAML с тенантами и index patterns | `osctltenants.yaml` |
| `--indexpatterns-recoverer-enabled` | `INDEXPATTERNS_RECOVERER_ENABLED` | Создавать `extracted_*` с ссылкой на data-source | `false` |
| `--dry-run` | `DRY_RUN` | Показать создаваемые index patterns без создания | `false` |

Примечание: Список тенантов берётся из `--indexpatterns-kibana-tenants-config` (`INDEXPATTERNS_KIBANA_TENANTS_CONFIG`).

**Ключи в конфиг файле:**
- `kibana_index_regex`
- `indexpatterns_kibana_multitenancy`
- `indexpatterns_kibana_tenants_config`
- `indexpatterns_recoverer_enabled`

### `datasource`

Создает Kibana data-source в нужных тенантах и при multidomain синхронизирует секреты.

| Флаг | Переменная окружения | Описание | Значение по умолчанию |
|------|---------------------|----------|--------------|
| `--kibana-user` | `KIBANA_API_USER` | Пользователь API Kibana | (пусто) |
| `--kibana-pass` | `KIBANA_API_PASS` | Пароль API Kibana | (пусто) |
| `--datasource-name` | `DATA_SOURCE_NAME` | Название data-source | `recoverer` |
| `--datasource-endpoint` | `DATASOURCE_ENDPOINT` | OpenSearch endpoint URL для data-source | `https://opendistro-recoverer:9200` |
| `--kube-namespace` | `KUBE_NAMESPACE` | Namespace для секретов | `infra-elklogs` |
| `--datasource-kibana-multidomain-enabled` | `DATASOURCE_KIBANA_MULTIDOMAIN_ENABLED` | Управление секретом `multi-certs` и перезапуск Kibana если было обновление сертификатов | `false` |
| `--datasource-remote-crt` | `DATASOURCE_REMOTE_CRT` | base64 сертификаты, разделённые \\| (используется при multidomain, будут объединены с `recoverer-certs/ca.crt` ) | (пусто) |
| `--datasource-kibana-multitenancy` | `DATASOURCE_KIBANA_MULTITENANCY` | Режим multitenancy | `false` |
| `--datasource-kibana-tenants-config` | `DATASOURCE_KIBANA_TENANTS_CONFIG` | Путь к YAML с тенантами и index patterns | `osctltenants.yaml` |
| `--dry-run` | `DRY_RUN` | Показать создание/обновление без изменений в Kibana/K8s | `false` |

Примечание: Список тенантов берётся из общего флага `--kibana-tenants-config` (`KIBANA_TENANTS_CONFIG`), а не из `--datasource-kibana-tenants-config`.

**Ключи в конфиг файле:**
- `datasource_name`
- `datasource_endpoint`
- `kube_namespace`
- `kibana_user`
- `kibana_pass`
- `datasource_kibana_multitenancy`
- `datasource_kibana_multidomain_enabled`
- `datasource_remote_crt`
- `datasource_kibana_tenants_config`

### `snapshotschecker`

Проверяет наличие снапшотов и отправляет алерт при отсутствии.

| Флаг | Переменная окружения | Описание | Значение по умолчанию |
|------|---------------------|----------|--------------|
| `--dry-run` | `DRY_RUN` | Только логирование; алерты не отправляются | `false` |

### `snapshotsbackfill`

Создает снапшоты для индексов, у которых их нет. Поддерживает два режима работы.

| Флаг | Переменная окружения | Описание | Значение по умолчанию |
|------|---------------------|----------|--------------|
| `--indices-list` | `SNAPSHOTS_BACKFILL_INDICES_LIST` | Список индексов через запятую для создания снапшотов, если не указан - обрабатывает все раньше 2 дней | (пусто) |
| `--dry-run` | `DRY_RUN` | Показать создаваемые снапшоты без выполнения | `false` |

**Режимы работы:**

1. **С параметром `--indices-list`**: Создает снапшоты для указанных индексов, если их нет. Индексы без даты в названии (в формате из конфига) пропускаются с логированием.

2. **Без параметра `--indices-list`**: Проверяет все индексы за позавчерашний день и все более старые дни (не включает сегодня и вчера). Все индексы должны иметь дату в названии в формате из конфига. Для таких индексов проверяется наличие снапшота и создается, если его нет.

**Алгоритм:**

- Составляется список индексов без снапшотов (учитываются даты и конфигурация `days_count` и `snapshot_count_s3`)
- Индексы группируются по датам по убыванию даты
- Для каждой группы по датам создаются снапшоты с паузой 10 минут между созданиями
- Перед каждым созданием проверяется наличие снапшота и его статус

**Конфигурация:**
- Использует `--osctl-indices-config` для централизованной конфигурации
- Учитывает `days_count` и `snapshot_count_s3` из конфига индексов
- Учитывает `s3_snapshots.unit_count.all` и `s3_snapshots.unit_count.unknown` из S3 конфига
- Поддерживает переопределение репозитория через `Repository` в конфиге индекса

### `danglingchecker`

Проверяет «висячие» индексы и отправляет алерт.

| Флаг | Переменная окружения | Описание | Значение по умолчанию |
|------|---------------------|----------|--------------|
| `--dry-run` | `DRY_RUN` | Только логирование; алерты не отправляются | `false` |

### `indicesdelete`

Удаляет индексы в соответствии с правилами `osctl-indices-config`.

| Флаг | Переменная окружения | Описание | Значение по умолчанию |
|------|---------------------|----------|--------------|
| `--indicesdelete-check-snapshots` | `INDICESDELETE_CHECK_SNAPSHOTS` | Проверять наличие валидных снапшотов перед удалением индексов, которые должны иметь снапшоты. Если `true` и не удалось получить информацию о снапшотах или `snap-repo` не настроен, джоба завершается с ошибкой | `true` |
| `--snap-repo` | `SNAPSHOT_REPOSITORY` | Название репозитория для снапшотов (обязателен если `indicesdelete-check-snapshots=true`) | (пусто) |
| `--dry-run` | `DRY_RUN` | Показать удаляемые индексы без удаления | `false` |

**Ключи в конфиг файле:**
- `indicesdelete_check_snapshots`
- `snapshot_repo`

В режиме multitenancy список тенантов берется из `--kibana-tenants-config` (`KIBANA_TENANTS_CONFIG`), файл обязателен.

